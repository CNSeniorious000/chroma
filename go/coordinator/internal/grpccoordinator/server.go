package grpccoordinator

import (
	"context"
	"errors"
	"time"

	"github.com/chroma/chroma-coordinator/internal/coordinator"
	"github.com/chroma/chroma-coordinator/internal/grpccoordinator/grpcutils"
	"github.com/chroma/chroma-coordinator/internal/memberlist_manager"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbcore"
	"github.com/chroma/chroma-coordinator/internal/proto/coordinatorpb"
	"github.com/chroma/chroma-coordinator/internal/utils"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"gorm.io/gorm"
)

type Config struct {
	// GRPC config
	BindAddress string

	// System catalog provider
	SystemCatalogProvider string

	// MetaTable config
	Username     string
	Password     string
	Address      string
	Port         int
	DBName       string
	MaxIdleConns int
	MaxOpenConns int

	// Pulsar config
	PulsarAdminURL  string
	PulsarTenant    string
	PulsarNamespace string

	// Kubernetes config
	KubernetesNamespace  string
	WorkerMemberlistName string

	// Assignment policy config can be "simple" or "rendezvous"
	AssignmentPolicy string

	// Watcher config
	WatchInterval time.Duration

	// Config for testing
	Testing bool
}

// Server wraps Coordinator with GRPC services.
//
// When Testing is set to true, the GRPC services will not be intialzed. This is
// convenient for end-to-end property based testing.
type Server struct {
	coordinatorpb.UnimplementedSysDBServer
	coordinator  coordinator.ICoordinator
	grpcServer   grpcutils.GrpcServer
	healthServer *health.Server
}

func New(config Config) (*Server, error) {
	if config.SystemCatalogProvider == "memory" {
		return NewWithGrpcProvider(config, grpcutils.Default, nil)
	} else if config.SystemCatalogProvider == "database" {
		dBConfig := dbcore.DBConfig{
			Username:     config.Username,
			Password:     config.Password,
			Address:      config.Address,
			Port:         config.Port,
			DBName:       config.DBName,
			MaxIdleConns: config.MaxIdleConns,
			MaxOpenConns: config.MaxOpenConns,
		}
		db, err := dbcore.Connect(dBConfig)
		if err != nil {
			return nil, err
		}
		return NewWithGrpcProvider(config, grpcutils.Default, db)
	} else {
		return nil, errors.New("invalid system catalog provider, only memory and database are supported")
	}
}

func NewWithGrpcProvider(config Config, provider grpcutils.GrpcProvider, db *gorm.DB) (*Server, error) {
	ctx := context.Background()
	s := &Server{
		healthServer: health.NewServer(),
	}

	var assignmentPolicy coordinator.CollectionAssignmentPolicy
	if config.AssignmentPolicy == "simple" {
		log.Info("Using simple assignment policy")
		assignmentPolicy = coordinator.NewSimpleAssignmentPolicy(config.PulsarTenant, config.PulsarNamespace)
	} else if config.AssignmentPolicy == "rendezvous" {
		log.Info("Using rendezvous assignment policy")

		err := utils.CreateTopics(config.PulsarAdminURL, config.PulsarTenant, config.PulsarNamespace, coordinator.Topics[:])
		if err != nil {
			log.Error("Failed to create topics", zap.Error(err))
			return nil, err
		}
		assignmentPolicy = coordinator.NewRendezvousAssignmentPolicy(config.PulsarTenant, config.PulsarNamespace)
	} else {
		return nil, errors.New("invalid assignment policy, only simple and rendezvous are supported")
	}
	coordinator, err := coordinator.NewCoordinator(ctx, assignmentPolicy, db)
	if err != nil {
		return nil, err
	}
	s.coordinator = coordinator
	s.coordinator.Start()
	if !config.Testing {
		memberlist_manager, err := createMemberlistManager(config)

		// Start the memberlist manager
		err = memberlist_manager.Start()
		if err != nil {
			return nil, err
		}

		s.grpcServer, err = provider.StartGrpcServer("coordinator", config.BindAddress, func(registrar grpc.ServiceRegistrar) {
			coordinatorpb.RegisterSysDBServer(registrar, s)
		})
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

func createMemberlistManager(config Config) (*memberlist_manager.MemberlistManager, error) {
	// TODO: Make this configuration
	log.Info("Starting memberlist manager")
	memberlist_name := config.WorkerMemberlistName
	namespace := config.KubernetesNamespace
	clientset, err := utils.GetKubernetesInterface()
	if err != nil {
		return nil, err
	}
	dynamicClient, err := utils.GetKubernetesDynamicInterface()
	if err != nil {
		return nil, err
	}
	nodeWatcher := memberlist_manager.NewKubernetesWatcher(clientset, namespace, "worker", config.WatchInterval)
	memberlistStore := memberlist_manager.NewCRMemberlistStore(dynamicClient, namespace, memberlist_name)
	memberlist_manager := memberlist_manager.NewMemberlistManager(nodeWatcher, memberlistStore)
	return memberlist_manager, nil
}

func (s *Server) Close() error {
	s.healthServer.Shutdown()
	return nil
}

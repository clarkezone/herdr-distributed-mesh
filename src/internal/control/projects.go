package control

import (
	"context"
	"errors"
	"fmt"
	"strings"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func RegisterProject(ctx context.Context, options Options, request *pb.RegisterProjectRequest) error {
	if request == nil || strings.TrimSpace(request.NodeInstanceId) == "" || strings.TrimSpace(request.ProjectId) == "" || strings.TrimSpace(request.CheckoutPath) == "" {
		return errors.New("project registration requires node, project, and checkout path")
	}
	return withProjects(ctx, options, func(client pb.FleetClient) error {
		return registerProjectWithClient(ctx, options, client, request)
	})
}

func Projects(ctx context.Context, options Options, nodeID string) error {
	return withProjects(ctx, options, func(client pb.FleetClient) error {
		return projectsWithClient(ctx, options, client, &pb.ListProjectsRequest{NodeInstanceId: nodeID})
	})
}

func GetProject(ctx context.Context, options Options, nodeID, projectID string) error {
	if strings.TrimSpace(nodeID) == "" || strings.TrimSpace(projectID) == "" {
		return errors.New("project lookup requires node and project")
	}
	return withProjects(ctx, options, func(client pb.FleetClient) error {
		return getProjectWithClient(ctx, options, client, &pb.GetProjectRequest{NodeInstanceId: nodeID, ProjectId: projectID})
	})
}

func withProjects(ctx context.Context, options Options, query func(pb.FleetClient) error) error {
	if strings.TrimSpace(options.RequiredServerTag) == "" {
		return errors.New("project requests require an expected server tag")
	}
	return withFleet(ctx, options, func(client pb.FleetClient, _ transport.SelfStatus) error {
		return query(client)
	})
}

func registerProjectWithClient(ctx context.Context, options Options, client pb.FleetClient, request *pb.RegisterProjectRequest) error {
	record, err := retryUnavailable(ctx, func() (*pb.ProjectRecord, error) {
		return client.RegisterProject(ctx, request)
	})
	if err != nil {
		return fmt.Errorf("register project: %w", err)
	}
	return writeProject(options, record)
}

func getProjectWithClient(ctx context.Context, options Options, client pb.FleetClient, request *pb.GetProjectRequest) error {
	record, err := retryUnavailable(ctx, func() (*pb.ProjectRecord, error) {
		return client.GetProject(ctx, request)
	})
	if err != nil {
		return fmt.Errorf("get project: %w", err)
	}
	return writeProject(options, record)
}

func projectsWithClient(ctx context.Context, options Options, client pb.FleetClient, request *pb.ListProjectsRequest) error {
	request = proto.Clone(request).(*pb.ListProjectsRequest)
	list := &pb.ProjectList{}
	seen := make(map[string]bool)
	for {
		page, err := retryUnavailable(ctx, func() (*pb.ProjectList, error) {
			return client.ListProjects(ctx, request)
		})
		if err != nil {
			return fmt.Errorf("list projects: %w", err)
		}
		if page == nil || len(page.Projects) > protocol.MaxProjectPageSize || len(list.Projects)+len(page.Projects) > 16384 {
			return errors.New("server returned an invalid or oversized project page")
		}
		list.Projects = append(list.Projects, page.Projects...)
		if page.NextPageToken == "" {
			break
		}
		if len(page.NextPageToken) > 512 || seen[page.NextPageToken] || len(page.Projects) == 0 {
			return errors.New("server returned an invalid project continuation")
		}
		seen[page.NextPageToken] = true
		request.PageToken = page.NextPageToken
	}
	return writeProjects(options, list)
}

func writeProjectJSON(options Options, value proto.Message) error {
	data, err := (protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}).Marshal(value)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(options.Output, string(data))
	return err
}

func writeProject(options Options, record *pb.ProjectRecord) error {
	if record == nil || record.Desired == nil {
		return errors.New("server returned an invalid project record")
	}
	if options.JSON {
		return writeProjectJSON(options, record)
	}
	desired := record.Desired
	var view humanView
	view.field("Project", desired.ProjectId)
	view.field("Target node", desired.NodeInstanceId)
	view.field("Readiness", readable(record.Readiness))
	view.field("Adoption", readable(record.AdoptionStatus))
	switch record.Readiness {
	case "pending":
		view.field("Next", "wait for the node to apply the desired configuration, then inspect this project again")
	case "offline":
		view.field("Next", "restore the node connection; the applied configuration is not a current readiness check")
	case "invalid":
		view.field("Next", "check the applied issue and project paths on the execution node, then correct the registration")
	case "unsupported":
		view.field("Next", "check compatible node and Herdr versions before using this project")
	}
	view.field("Desired generation", fmt.Sprint(desired.Generation))
	view.field("Desired checkout", desired.CheckoutPath)
	view.field("Desired worktree root", desired.WorktreeRoot)
	if applied := record.Applied; applied != nil {
		view.field("Applied generation", fmt.Sprint(applied.Generation))
		view.field("Applied status", readable(applied.Status))
		view.field("Applied checkout", applied.CheckoutPath)
		view.field("Applied worktree root", applied.WorktreeRoot)
		view.field("Applied issue", HumanDetail(applied.ErrorCode))
		if applied.Generation != desired.Generation {
			view.field("Configuration", "desired and applied generations differ; wait for the node to apply the current registration")
		}
	} else {
		view.field("Applied configuration", "none reported; wait for the connected node to acknowledge the registration")
	}
	_, err := fmt.Fprintln(options.Output, view.String())
	return err
}

func writeProjects(options Options, list *pb.ProjectList) error {
	if list == nil {
		return errors.New("server returned an invalid project list")
	}
	for _, record := range list.Projects {
		if record == nil || record.Desired == nil {
			return errors.New("server returned an invalid project record")
		}
	}
	if options.JSON {
		return writeProjectJSON(options, list)
	}
	if len(list.Projects) == 0 {
		_, err := fmt.Fprintln(options.Output, "no registered projects")
		return err
	}
	for _, record := range list.Projects {
		if err := writeProject(options, record); err != nil {
			return err
		}
	}
	return nil
}

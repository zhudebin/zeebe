// Copyright © 2018 Camunda Services GmbH (info@camunda.com)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package commands

import (
	"context"
	"github.com/zeebe-io/zeebe/clients/go/pkg/pb"
)

type DispatchUpdateClusterSizeCommitCommand interface {
	Send(context.Context) (*pb.UpdateClusterSizeCommitResponse, error)
}

type UpdateClusterSizeCommitCommandStep1 interface {
	NewClusterSize(int32) UpdateClusterSizeCommitCommandStep2
}

type UpdateClusterSizeCommitCommandStep2 interface {
	DispatchUpdateClusterSizeCommitCommand

	NodeId(int32) DispatchUpdateClusterSizeCommitCommand
}

type UpdateClusterSizeCommitCommand struct {
	Command
	request pb.UpdateClusterSizeCommitRequest
}

func (cmd *UpdateClusterSizeCommitCommand) NewClusterSize(newClusterSize int32) UpdateClusterSizeCommitCommandStep2 {
	cmd.request.NewClusterSize = newClusterSize
	return cmd
}

func (cmd *UpdateClusterSizeCommitCommand) NodeId(nodeId int32) DispatchUpdateClusterSizeCommitCommand {
	cmd.request.NodeId = nodeId
	return cmd
}

func (cmd *UpdateClusterSizeCommitCommand) Send(ctx context.Context) (*pb.UpdateClusterSizeCommitResponse, error) {
	response, err := cmd.gateway.UpdateClusterSizeCommit(ctx, &cmd.request)
	if cmd.shouldRetry(ctx, err) {
		return cmd.Send(ctx)
	}

	return response, err
}

func NewUpdateClusterSizeCommitCommand(gateway pb.GatewayClient, pred retryPredicate) UpdateClusterSizeCommitCommandStep1 {
	return &UpdateClusterSizeCommitCommand{
		request: pb.UpdateClusterSizeCommitRequest{
		},
		Command: Command{
			gateway:     gateway,
			shouldRetry: pred,
		},
	}
}

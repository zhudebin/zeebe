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

type DispatchUpdateClusterSizeInitCommand interface {
	Send(context.Context) (*pb.UpdateClusterSizeInitResponse, error)
}

type UpdateClusterSizeInitCommandStep1 interface {
	NewClusterSize(int32) UpdateClusterSizeInitCommandStep2
}

type UpdateClusterSizeInitCommandStep2 interface {
	DispatchUpdateClusterSizeInitCommand

	NodeId(int32) DispatchUpdateClusterSizeInitCommand
}

type UpdateClusterSizeInitCommand struct {
	Command
	request pb.UpdateClusterSizeInitRequest
}

func (cmd *UpdateClusterSizeInitCommand) NewClusterSize(newClusterSize int32) UpdateClusterSizeInitCommandStep2 {
	cmd.request.NewClusterSize = newClusterSize
	return cmd
}

func (cmd *UpdateClusterSizeInitCommand) NodeId(nodeId int32) DispatchUpdateClusterSizeInitCommand {
	cmd.request.NodeId = nodeId
	return cmd
}

func (cmd *UpdateClusterSizeInitCommand) Send(ctx context.Context) (*pb.UpdateClusterSizeInitResponse, error) {
	response, err := cmd.gateway.UpdateClusterSizeInit(ctx, &cmd.request)
	if cmd.shouldRetry(ctx, err) {
		return cmd.Send(ctx)
	}

	return response, err
}

func NewUpdateClusterSizeInitCommand(gateway pb.GatewayClient, pred retryPredicate) UpdateClusterSizeInitCommandStep1 {
	return &UpdateClusterSizeInitCommand{
		request: pb.UpdateClusterSizeInitRequest{
		},
		Command: Command{
			gateway:     gateway,
			shouldRetry: pred,
		},
	}
}

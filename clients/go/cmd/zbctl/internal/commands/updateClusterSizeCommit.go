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

package commands

import (
	"context"
	"github.com/spf13/cobra"
	"log"
)

var (
	clusterSizeCommit  int32
	nodeIdFlagCommit int32
)

var updateClusterSizeCommitCmd = &cobra.Command{
	Use:     "commit <size>",
	Short:   "Finalize cluster size update in a target node. This should be called only after 'update clusterSize init' is called for all nodes",
	Args:    intArg(&clusterSizeCommit),
	PreRunE: initClient,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		defer cancel()

		_, err := client.NewUpdateClusterSizeCommitCommand().NewClusterSize(clusterSizeCommit).NodeId(nodeIdFlagCommit).Send(ctx)
		if err == nil {
			log.Println("Updated cluster size initiated in node", clusterSizeCommit, ",", nodeIdFlagCommit)
		}

		return err
	},
}

func init() {
	updateClusterSizeCmd.AddCommand(updateClusterSizeCommitCmd)
	updateClusterSizeCommitCmd.Flags().Int32Var(&nodeIdFlagCommit, "nodeId", -1, "Specify target node id to send the command to")
	if err := updateClusterSizeCommitCmd.MarkFlagRequired("nodeId"); err != nil {
		panic(err)
	}
}

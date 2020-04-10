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
	clusterSize  int32
	nodeIdFlag int32
)

var updateClusterSizeInitCmd = &cobra.Command{
	Use:     "init <size>",
	Short:   "Initiate cluster size update in a target node",
	Args:    intArg(&clusterSize),
	PreRunE: initClient,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		defer cancel()

		_, err := client.NewUpdateClusterSizeInitCommand().NewClusterSize(clusterSize).NodeId(nodeIdFlag).Send(ctx)
		if err == nil {
			log.Println("Updated cluster size initiated in node", clusterSize, ",", nodeIdFlag)
		}

		return err
	},
}

func init() {
	updateClusterSizeCmd.AddCommand(updateClusterSizeInitCmd)
	updateClusterSizeInitCmd.Flags().Int32Var(&nodeIdFlag, "nodeId", -1, "Specify target node id to send the command to")
	if err := updateClusterSizeInitCmd.MarkFlagRequired("nodeId"); err != nil {
		panic(err)
	}
}

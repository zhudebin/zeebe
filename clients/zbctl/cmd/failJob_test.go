package cmd

import (
    "bytes"
    "fmt"
    "github.com/spf13/cobra"
    "github.com/zeebe-io/zeebe/clients/go"
    "github.com/zeebe-io/zeebe/clients/go/commands"
    "github.com/zeebe-io/zeebe/clients/go/worker"
    "testing"
    "time"
)

type MockZBClient struct {}

func (*MockZBClient) NewTopologyCommand() *commands.TopologyCommand {
    panic("implement me")
}

func (*MockZBClient) NewDeployWorkflowCommand() *commands.DeployCommand {
    panic("implement me")
}

func (*MockZBClient) NewCreateInstanceCommand() commands.CreateInstanceCommandStep1 {
    panic("implement me")
}

func (*MockZBClient) NewCancelInstanceCommand() commands.CancelInstanceStep1 {
    panic("implement me")
}

func (*MockZBClient) NewUpdatePayloadCommand() commands.UpdatePayloadCommandStep1 {
    panic("implement me")
}

func (*MockZBClient) NewPublishMessageCommand() commands.PublishMessageCommandStep1 {
    panic("implement me")
}

func (*MockZBClient) NewCreateJobCommand() commands.CreateJobCommandStep1 {
    panic("implement me")
}

func (*MockZBClient) NewActivateJobsCommand() commands.ActivateJobsCommandStep1 {
    panic("implement me")
}

func (*MockZBClient) NewCompleteJobCommand() commands.CompleteJobCommandStep1 {
    panic("implement me")
}

func (*MockZBClient) NewFailJobCommand() commands.FailJobCommandStep1 {
    panic("implement me")
}

func (*MockZBClient) NewUpdateJobRetriesCommand() commands.UpdateJobRetriesCommandStep1 {
    panic("implement me")
}

func (*MockZBClient) NewListWorkflowsCommand() commands.ListWorkflowsStep1 {
    panic("implement me")
}

func (*MockZBClient) NewGetWorkflowCommand() commands.GetWorkflowStep1 {
    panic("implement me")
}

func (*MockZBClient) NewJobWorker() worker.JobWorkerBuilderStep1 {
    panic("implement me")
}

func (*MockZBClient) SetRequestTimeout(time.Duration) zbc.ZBClient {
    panic("implement me")
}

func (*MockZBClient) Close() error {
    panic("implement me")
}

func NewMockZBClient() *MockZBClient {
    return &MockZBClient{}
}

func executeCommand(root *cobra.Command, args ...string) (output string, err error) {
    _, output, err = executeCommandC(root, args...)
    return output, err
}

func executeCommandC(root *cobra.Command, args ...string) (c *cobra.Command, output string, err error) {
    buf := new(bytes.Buffer)
    root.SetOutput(buf)
    root.SetArgs(args)

    c, err = root.ExecuteC()

    return c, buf.String(), err
}

func executeZbCtlCommand(args... string) (output string, err error) {
    rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
        fmt.Println("mock me")
        client = NewMockZBClient()
        return nil
    }
    testRootCmd := rootCmd
    return executeCommand(testRootCmd, args...)
}

func TestFailJob(t *testing.T) {
    output, err := executeZbCtlCommand("fail", "job", "123", "--retries", "3")
    if err != nil {
        t.Error(err)
    }
    fmt.Println(output)
}

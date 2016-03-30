package file

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/hashicorp/terraform/communicator"
	"github.com/hashicorp/terraform/helper/config"
	"github.com/hashicorp/terraform/terraform"
	"github.com/mitchellh/go-homedir"
)

// ResourceProvisioner represents a file provisioner
type ResourceProvisioner struct{}

// Apply executes the file provisioner
func (p *ResourceProvisioner) Apply(
	o terraform.UIOutput,
	s *terraform.InstanceState,
	c *terraform.ResourceConfig) error {
	// Get a new communicator
	comm, err := communicator.New(s)
	if err != nil {
		return err
	}

	dRaw := c.Config["destination"]
	dst, ok := dRaw.(string)
	if !ok {
		return fmt.Errorf("Unsupported 'destination' type! Must be string.")
	}

	if c.IsSet("source") && c.IsSet("content") {
		return fmt.Errorf("Unsupported use of both source and content at the same time")
	}

	if c.IsSet("source") {
		// Get the source and destination
		sRaw := c.Config["source"]
		src, ok := sRaw.(string)
		if !ok {
			return fmt.Errorf("Unsupported 'source' type! Must be string.")
		}

		src, err = homedir.Expand(src)
		if err != nil {
			return err
		}

		return p.copyFilesFromSource(comm, src, dst)
	}
	if c.IsSet("content") {
		cRaw := c.Config["content"]
		cont, ok := cRaw.(string)
		if !ok {
			return fmt.Errorf("Unsupported 'content' type! Must be string.")
		}
		return p.createFileFromContent(comm, cont, dst)
	}
	return fmt.Errorf("You must have eather source or content set")
}

// Validate checks if the required arguments are configured
func (p *ResourceProvisioner) Validate(c *terraform.ResourceConfig) (ws []string, es []error) {
	v := &config.Validator{
		Required: []string{
			"destination",
		},
		Optional: []string{
			"source",
			"content",
		},
	}
	return v.Validate(c)
}

// Copy content to destination
func (p *ResourceProvisioner) createFileFromContent(comm communicator.Communicator, content string, dst string) error {
	// Wait and retry until we establish the connection
	err := retryFunc(comm.Timeout(), func() error {
		err := comm.Connect(nil)
		return err
	})
	if err != nil {
		return err
	}
	defer comm.Disconnect()

	f := bytes.NewBuffer([]byte(content))

	err = comm.Upload(dst, f)
	if err != nil {
		return fmt.Errorf("Upload failed: %v", err)
	}
	return err
}

// copyFiles is used to copy the files from a source to a destination
func (p *ResourceProvisioner) copyFilesFromSource(comm communicator.Communicator, src, dst string) error {
	// Wait and retry until we establish the connection
	err := retryFunc(comm.Timeout(), func() error {
		err := comm.Connect(nil)
		return err
	})
	if err != nil {
		return err
	}
	defer comm.Disconnect()

	info, err := os.Stat(src)
	if err != nil {
		return err
	}

	// If we're uploading a directory, short circuit and do that
	if info.IsDir() {
		if err := comm.UploadDir(dst, src); err != nil {
			return fmt.Errorf("Upload failed: %v", err)
		}
		return nil
	}

	// We're uploading a file...
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()

	err = comm.Upload(dst, f)
	if err != nil {
		return fmt.Errorf("Upload failed: %v", err)
	}
	return err
}

// retryFunc is used to retry a function for a given duration
func retryFunc(timeout time.Duration, f func() error) error {
	finish := time.After(timeout)
	for {
		err := f()
		if err == nil {
			return nil
		}
		log.Printf("Retryable error: %v", err)

		select {
		case <-finish:
			return err
		case <-time.After(3 * time.Second):
		}
	}
}

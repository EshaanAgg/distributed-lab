package utils

import (
	"fmt"
	"os"
)

// Deletes all the contents of the directory `dirPath`
func EmptyDirectory(dirPath string) error {
	err := os.RemoveAll(dirPath)
	if err != nil {
		return fmt.Errorf("failed to remove the directory %v: %v", dirPath, err)
	}

	err = os.Mkdir(dirPath, 0777)
	if err != nil {
		return fmt.Errorf("failed to create the directory %v: %v", dirPath, err)
	}

	return nil
}

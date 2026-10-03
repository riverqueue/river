// Command check-packages checks Maven archives for development files and
// missing runtime resources. Run it from the repository root with
// `make check/java/package`.
package main

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var forbiddenPath = regexp.MustCompile(`(^|/)(fixtures?|testdata|tests|conformance)/|(?:Test|Tests)(?:\$[^/]*)?\.(class|java)$`)

func main() {
	if err := run("java"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func checkArchive(path, resources string) error {
	archive, err := zip.OpenReader(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer archive.Close()

	names := make(map[string]bool, len(archive.File))
	for _, file := range archive.File {
		name := file.Name
		if forbiddenPath.MatchString(name) || (strings.HasPrefix(name, "com/riverqueue/") && strings.HasSuffix(name, ".json")) {
			return fmt.Errorf("%s contains development content: %s", path, name)
		}
		names[name] = true
	}
	if resources == "" {
		return nil
	}

	resourceFS := os.DirFS(resources)
	if err := fs.WalkDir(resourceFS, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		expected, err := fs.ReadFile(resourceFS, name)
		if err != nil {
			return err
		}
		if !names[name] {
			return fmt.Errorf("missing runtime resource %s", name)
		}
		actual, err := fs.ReadFile(archive, name)
		if err != nil {
			return fmt.Errorf("read runtime resource %s: %w", name, err)
		}
		if !bytes.Equal(actual, expected) {
			return fmt.Errorf("stale runtime resource %s", name)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	if !names["com/riverqueue/Client.class"] {
		return fmt.Errorf("%s: missing Client class", path)
	}
	return nil
}

func run(root string) error {
	contents, err := os.ReadFile(filepath.Join(root, "pom.xml"))
	if err != nil {
		return fmt.Errorf("read Maven project: %w", err)
	}
	var project struct {
		Version string `xml:"version"`
	}
	if err := xml.Unmarshal(contents, &project); err != nil {
		return fmt.Errorf("parse Maven project: %w", err)
	}
	if project.Version == "" {
		return fmt.Errorf("%s/pom.xml: missing project version", root)
	}

	for _, artifact := range []struct {
		path    string
		runtime bool
	}{
		{"river/target/river-" + project.Version + ".jar", true},
		{"river/target/river-" + project.Version + "-sources.jar", false},
		{"cli/target/river-cli.jar", false},
		{"cli/target/river-cli-sources.jar", false},
		{"cli/target/river-cli-" + project.Version + "-all.jar", true},
	} {
		var resources string
		if artifact.runtime {
			resources = filepath.Join(root, "river/src/main/resources")
		}
		if err := checkArchive(filepath.Join(root, artifact.path), resources); err != nil {
			return err
		}
		fmt.Printf("Java package verified: %s\n", artifact.path)
	}
	return nil
}

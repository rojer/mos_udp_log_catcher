/*
 * Copyright (c) 2022 Deomid "rojer" Ryabkov
 * All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"text/template"
	"time"

	"github.com/juju/errors"
	klog "k8s.io/klog/v2"
)

const (
	deviceLogName       = "{{.DeviceIDSafe}}.{{.Year}}{{.Month}}{{.Day}}.log"
	latestDeviceLogName = "{{.DeviceIDSafe}}.log"
)

type deviceInfo struct {
	fd       *os.File
	fname    string
	lastUsed time.Time
}

func (di *deviceInfo) Open(nameTmpl, latestNameTmpl *template.Template, li *LineInfo) error {
	fname, err := execTmpl(nameTmpl, li)
	if err != nil {
		return errors.Annotatef(err, "Failed to execute file name template: %v", err)
	}
	if di.fname == fname && di.fd != nil {
		return nil
	}
	if di.fd != nil {
		di.Close()
	}
	di.fname = fname
	if err := os.MkdirAll(filepath.Dir(fname), 0o755); err != nil {
		return errors.Annotatef(err, "failed to create log dir")
	}
	if fd, err := os.OpenFile(di.fname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644); err != nil {
		return errors.Trace(err)
	} else {
		klog.Infof("Opened %s", di.fname)
		di.fd = fd
	}
	if latestNameTmpl != nil {
		latestName, err := execTmpl(latestNameTmpl, li)
		if err != nil {
			return errors.Annotatef(err, "Failed to execute file name template: %v", err)
		}
		target, err := os.Readlink(latestName)
		latestBase := filepath.Base(di.fname)
		if err != nil || target != latestBase {
			os.Remove(latestName)
			if err = os.Symlink(latestBase, latestName); err != nil {
				klog.Errorf("Failed to symlink %s to %s", di.fname, latestBase)
			} else {
				klog.Infof("%s -> %s", latestName, latestBase)
			}
		}
	}
	return nil
}

func (di *deviceInfo) Close() error {
	if di.fd != nil {
		klog.Infof("Closed %s", di.fname)
		err := di.fd.Close()
		di.fd = nil
		return err
	}
	return nil
}

type FileManager struct {
	nameTmpl       *template.Template
	latestNameTmpl *template.Template
	recordTmpl     *template.Template
	mu             sync.Mutex
	devices        map[string]*deviceInfo
}

func execTmpl(t *template.Template, li *LineInfo) (string, error) {
	nameBuf := bytes.NewBuffer(nil)
	if err := t.Execute(nameBuf, li); err != nil {
		return "", err
	}
	return string(nameBuf.Bytes()), nil
}

func (fm *FileManager) WriteLine(li *LineInfo) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	di, found := fm.devices[li.DeviceIDSafe]
	if !found {
		di = &deviceInfo{
			lastUsed: time.Now(),
		}
		fm.devices[li.DeviceIDSafe] = di
	}
	if err := di.Open(fm.nameTmpl, fm.latestNameTmpl, li); err != nil {
		klog.Errorf("Failed to open log file: %v", err)
		return
	}
	fm.recordTmpl.Execute(di.fd, li)
	di.fd.Write([]byte{'\n'})
	di.lastUsed = time.Now()
}

func NewFileManager(dir, recordTmpl string) (*FileManager, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, errors.Annotatef(err, "failed to create log dir")
	}
	fm := &FileManager{
		devices: make(map[string]*deviceInfo),
	}
	var err error
	if fm.nameTmpl, err = template.New("filename").Parse(filepath.Join(dir, "{{.DeviceIDSafe}}", deviceLogName)); err != nil {
		return nil, errors.Annotatef(err, "invalid file name template")
	}
	if fm.latestNameTmpl, err = template.New("filename").Parse(filepath.Join(dir, "{{.DeviceIDSafe}}", latestDeviceLogName)); err != nil {
		return nil, errors.Annotatef(err, "invalid file name template")
	}
	if fm.recordTmpl, err = template.New("file").Parse(*flagFileFormat); err != nil {
		return nil, errors.Annotatef(err, "invalid file record format template")
	}
	return fm, nil
}

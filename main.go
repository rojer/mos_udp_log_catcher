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
	stdFlag "flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"text/template"
	"time"

	"github.com/juju/errors"
	flag "github.com/spf13/pflag"
	klog "k8s.io/klog/v2"
)

var (
	flagListenAddr   = flag.String("listen-addr", "", "Address to listen on; udp://:port/ or udp://addr:port/")
	flagTimestamp    = flag.String("timestamp-format", "StampMilli", "Format of the timestamp, see https://pkg.go.dev/time#pkg-constants")
	flagStdout       = flag.Bool("stdout", false, "Log incoming messages to stdout")
	flagStdoutFormat = flag.String("stdout-format", "{{.TimestampStr}} {{.DeviceID}} {{.Src}} {{.LevelChar}} {{.Msg}}", "Format of stdout records")
	flagLogDir       = flag.String("log-dir", "", "Log incoming messages to per-device files in this directory")
	flagFileFormat   = flag.String("file-format", "{{.TimestampStr}} {{.Src}} {{.LevelChar}} {{.Msg}}", "Format of file records")
)

// UDP log line format is:
// device_id seq_no uptime fd level|msg
// One or more lines per packet. No splitting between packets.

var (
	safeChars  [256]bool
	stdoutTmpl *template.Template
	fileTmpl   *template.Template
)

func UDPLog() error {
	if *flagListenAddr == "" {
		return fmt.Errorf("--listen-addr is required")
	}
	purl, err := url.Parse(*flagListenAddr)
	if err != nil {
		return errors.Annotatef(err, "invalid --listen-addr")
	}
	if purl.Scheme != "udp" {
		return fmt.Errorf("scheme must be udp://")
	}
	p, err := strconv.Atoi(purl.Port())
	if err != nil {
		return errors.Errorf("invalid UDP port format, must be udp://:port/ or udp://ip:port/")
	}
	addr := net.UDPAddr{
		IP:   net.ParseIP(purl.Hostname()),
		Port: p,
	}
	udpc, err := net.ListenUDP("udp", &addr)
	if err != nil {
		return errors.Annotatef(err, "failed to open listner at %+v", addr)
	}
	defer udpc.Close()
	if len(*flagTimestamp) > 0 {
		tsFormat = ParseTimeStampFormatSpec(*flagTimestamp)
	}
	if *flagStdout {
		if stdoutTmpl, err = template.New("filename").Parse(*flagStdoutFormat); err != nil {
			return errors.Annotatef(err, "invalid --udp-log-stdout-format template")
		}
	}
	var fm *FileManager
	if len(*flagLogDir) > 0 {
		if fm, err = NewFileManager(*flagLogDir, *flagFileFormat); err != nil {
			return errors.Trace(err)
		}
	}
	if addr.IP != nil {
		klog.Infof("Listening on UDP %s:%d...", addr.IP, addr.Port)
	} else {
		klog.Infof("Listening on UDP port %d...", addr.Port)
	}
	for {
		pkt := make([]byte, 1500)
		n, src, err := udpc.ReadFromUDP(pkt)
		if err != nil {
			return errors.Annotatef(err, "socket read error")
		}
		ts := time.Now()
		buf := bytes.NewBuffer(pkt[:n])
		for buf.Len() > 10 {
			line, _ := buf.ReadBytes('\n')
			line = bytes.TrimRight(line, "\r\n")
			if err = processLine(ts, src, line, fm); err != nil {
				klog.Errorf("invalid log message %q: %v", string(line), err)
			}
		}
	}
	return nil
}

type LineInfo struct {
	Src       *net.UDPAddr
	Timestamp time.Time
	DeviceID  string
	SeqNum    uint64
	UptimeMs  uint64
	FD        uint
	Level     uint
	Msg       string
	// These are derived.
	TimestampStr string // Formatted acoording to --timestamp format
	DeviceIDSafe string // Sanitized, suitable for use in filenames.
	Year         string // YYYY
	Month        string // mm
	Day          string // dd
	LevelChar    string // E, W, I, D, V
}

func parseLine(ts time.Time, src *net.UDPAddr, line []byte) (*LineInfo, error) {
	infoStr, msg, found := bytes.Cut(line, []byte("|"))
	if !found {
		return nil, fmt.Errorf("missing msg delimiter")
	}
	parts := bytes.Split(infoStr, []byte(" "))
	if len(parts) != 5 {
		return nil, fmt.Errorf("invalid number of parts")
	}
	var li LineInfo
	if len(parts[0]) > 0 && len(parts[0]) <= 50 {
		li.DeviceID = string(parts[0])
	} else {
		return nil, fmt.Errorf("invalid device id")
	}
	if v, err := strconv.ParseUint(string(parts[1]), 10, 64); err == nil {
		li.SeqNum = v
	} else {
		return nil, fmt.Errorf("invalid seqnum")
	}
	if v, err := strconv.ParseFloat(string(parts[2]), 64); err == nil {
		li.UptimeMs = uint64(v * 1000)
	} else {
		return nil, fmt.Errorf("invalid uptime")
	}
	if v, err := strconv.ParseUint(string(parts[3]), 10, 32); err == nil {
		li.FD = uint(v)
	} else {
		return nil, fmt.Errorf("invalid fd")
	}
	if v, err := strconv.ParseUint(string(parts[4]), 10, 32); err == nil {
		li.Level = uint(v)
	} else {
		return nil, fmt.Errorf("invalid level")
	}
	for i, c := range parts[0] {
		if !safeChars[c] {
			parts[0][i] = '_'
		}
	}
	li.Src = src
	li.DeviceIDSafe = string(parts[0])
	li.Timestamp = ts
	li.Msg = string(msg)
	ds := ts.Format("20060102")
	li.Year = ds[:4]
	li.Month = ds[4:6]
	li.Day = ds[6:8]
	switch li.Level {
	case 0:
		li.LevelChar = "E"
	case 1:
		li.LevelChar = "W"
	case 2:
		li.LevelChar = "I"
	case 3:
		li.LevelChar = "D"
	case 4:
		li.LevelChar = "V"
	default:
		li.LevelChar = fmt.Sprintf("%d", li.Level%10)
	}
	li.TimestampStr = FormatTimestamp(ts)
	return &li, nil
}

func processLine(ts time.Time, src *net.UDPAddr, line []byte, fm *FileManager) error {
	li, err := parseLine(ts, src, line)
	if err != nil {
		return errors.Trace(err)
	}
	if stdoutTmpl != nil {
		stdoutTmpl.Execute(os.Stdout, li)
		os.Stdout.Write([]byte{'\n'})
	}
	if fm != nil {
		fm.WriteLine(li)
	}
	return nil
}

func main() {
	klog.InitFlags(nil)
	flag.CommandLine.AddGoFlag(stdFlag.CommandLine.Lookup("v"))
	flag.CommandLine.AddGoFlag(stdFlag.CommandLine.Lookup("logtostderr"))
	flag.CommandLine.Set("logtostderr", "true")
	flag.Parse()
	defer klog.Flush()

	for i := 0; i < 256; i++ {
		c := byte(i)
		safeChars[i] = ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') ||
			c == '-' || c == '_' || c == '.' || c == ',' || c == ' ')
	}

	if err := UDPLog(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", errors.ErrorStack(err))
		os.Exit(1)
	}
}

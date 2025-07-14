package main

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/richardwilkes/toolbox/v2/i18n"
	"github.com/richardwilkes/toolbox/v2/xflag"
	"github.com/richardwilkes/toolbox/v2/xio"
	"github.com/richardwilkes/toolbox/v2/xos"
	"github.com/richardwilkes/toolbox/v2/xstrings"
	"github.com/richardwilkes/toolbox/v2/xterm"
	"github.com/yookoala/realpath"
)

var (
	extensions             []string
	hidden                 bool
	remove                 bool
	removeOnlyFromLast     bool
	caseSensitive          bool
	filesProcessed         int32
	filesUnableToProcess   int32
	bytesProcessed         int64
	duplicatesFound        int32
	duplicateBytes         int64
	lock                   sync.Mutex
	hashes                 = make(map[[32]byte][]string)
	removed                []string
	unableToRemove         []string
	removeOnlyFromLastRoot string
)

func main() {
	xos.AppName = "Find Duplicates"
	xos.AppVersion = "1.0.4"
	xos.CopyrightHolder = "Richard Wilkes"
	xos.CopyrightStartYear = "2018"
	xos.License = "Mozilla Public License Version 2.0"
	xflag.SetUsage(nil, "", "[dir]...")
	extList := flag.String("ext", "",
		i18n.Text("Limit processing to just files with these `extensions`. Separate multiple values with commas"))
	flag.BoolVar(&hidden, "hidden", false,
		i18n.Text("Process files and directories that start with a period. Hidden files are ignored by default"))
	flag.BoolVar(&remove, "delete", false,
		i18n.Text("Delete all duplicates found. The first copy encountered will be preserved"))
	flag.BoolVar(&removeOnlyFromLast, "last", false,
		i18n.Text("When deleting duplicates, only delete those found within the last directory tree specified on the command line"))
	flag.BoolVar(&caseSensitive, "case", false, i18n.Text("Extensions are case-sensitive"))
	xflag.AddVersionFlags()
	xflag.Parse()
	paths := flag.Args()

	// If no paths specified, use the current directory
	if len(paths) == 0 {
		wd, err := os.Getwd()
		if err != nil {
			xos.ExitWithMsg(i18n.Text("Unable to determine current working directory."))
		}
		paths = append(paths, wd)
	}

	// Determine the actual root paths and prune out paths that are a subset of another
	set := make(map[string]int)
	order := 0
	for _, path := range paths {
		actual, err := realpath.Realpath(path)
		if err != nil {
			xos.ExitWithMsg(fmt.Sprintf(i18n.Text("Unable to determine real path for '%s'."), path))
		}
		if _, exists := set[actual]; !exists {
			add := true
			for one := range set {
				prefixed := strings.HasPrefix(rel(one, actual), "..")
				if prefixed != strings.HasPrefix(rel(actual, one), "..") {
					if prefixed {
						delete(set, one)
					} else {
						add = false
						break
					}
				}
			}
			if add {
				set[actual] = order
				order++
			}
		}
		removeOnlyFromLastRoot = actual
	}

	// Setup progress monitoring
	w := xterm.NewAnsiWriter(os.Stdout)
	w.Clear()
	w.HideCursor()
	xos.RunAtExit(func() {
		w.ShowCursor()
	})
	status(w)
	done := make(chan chan bool)
	go progress(w, done)

	// Ensure extensions are properly formatted
	var ext []string
	for _, one := range strings.Split(*extList, ",") {
		one = strings.TrimSpace(one)
		if one == "" {
			continue
		}
		if !caseSensitive {
			one = strings.ToLower(one)
		}
		if !strings.HasPrefix(one, ".") {
			one = "." + one
		}
		if one != "." {
			ext = append(ext, one)
		}
	}
	extensions = ext

	// Process the paths
	type po struct {
		path  string
		order int
	}
	list := make([]po, 0, len(set))
	for path, order := range set {
		list = append(list, po{path: path, order: order})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].order < list[j].order })
	for _, one := range list {
		xos.ExitIfErr(filepath.Walk(one.path, walker))
	}
	waitDone := make(chan bool)
	done <- waitDone
	<-waitDone

	// Report
	status(w)
	w.WriteByte('\n')
	if remove {
		summarizeList(w, i18n.Text("Removed"), removed)
		summarizeList(w, i18n.Text("Unable to remove"), unableToRemove)
	} else {
		var dups []string
		m := make(map[string][]string)
		for _, v := range hashes {
			if len(v) > 1 {
				dups = append(dups, v[0])
				m[v[0]] = v[1:]
			}
		}
		if len(dups) > 0 {
			for _, dup := range dups {
				w.WriteByte('\n')
				w.WriteString(dup)
				w.WriteByte('\n')
				for _, one := range m[dup] {
					w.WriteString(one)
					w.WriteByte('\n')
				}
			}
		} else {
			w.WriteByte('\n')
			w.WriteString(i18n.Text("No duplicates found."))
			w.WriteByte('\n')
		}
	}
}

func rel(base, target string) string {
	path, err := filepath.Rel(base, target)
	xos.ExitIfErr(err)
	return path
}

func progress(w *xterm.AnsiWriter, done chan chan bool) {
	for {
		select {
		case response := <-done:
			w.ShowCursor()
			response <- true
			return
		case <-time.After(time.Second / 4):
			status(w)
		}
	}
}

func status(w *xterm.AnsiWriter) {
	count := atomic.LoadInt32(&filesProcessed)
	bytes := atomic.LoadInt64(&bytesProcessed)
	w.Position(1, 1)
	w.EraseLine()
	w.Reset()
	w.WriteString(i18n.Text("Examined"))
	writeFileCount(w, int64(count), false)
	w.WriteByte(' ')
	w.WriteString(i18n.Text("containing"))
	writeByteCount(w, bytes)
	w.WriteString(".\n")

	count = atomic.LoadInt32(&duplicatesFound)
	bytes = atomic.LoadInt64(&duplicateBytes)
	w.EraseLine()
	w.WriteString(i18n.Text("Found"))
	writeFileCount(w, int64(count), true)
	if count > 0 {
		w.WriteByte(' ')
		w.WriteString(i18n.Text("containing"))
		writeByteCount(w, bytes)
	}
	w.WriteByte('.')
}

func writeFileCount(w *xterm.AnsiWriter, count int64, dupes bool) {
	w.WriteByte(' ')
	w.Bold()
	w.Yellow()
	w.WriteString(humanize.Comma(count))
	w.Reset()
	if dupes {
		w.WriteByte(' ')
		w.WriteString(i18n.Text("duplicate"))
	}
	w.WriteByte(' ')
	if count == 1 {
		w.WriteString(i18n.Text("file"))
	} else {
		w.WriteString(i18n.Text("files"))
	}
}

func writeByteCount(w *xterm.AnsiWriter, bytes int64) {
	w.WriteByte(' ')
	w.Bold()
	w.Yellow()
	w.WriteString(humanize.Comma(bytes))
	w.Reset()
	w.WriteByte(' ')
	if bytes == 1 {
		w.WriteString(i18n.Text("byte"))
	} else {
		w.WriteString(i18n.Text("bytes"))
	}
}

func summarizeList(w *xterm.AnsiWriter, prefix string, list []string) {
	if len(list) > 0 {
		sort.Slice(list, func(i, j int) bool { return xstrings.NaturalLess(list[i], list[j], true) })
		w.WriteByte('\n')
		w.WriteString(prefix)
		writeFileCount(w, int64(len(list)), false)
		w.WriteString(":\n")
		for _, one := range list {
			w.WriteString(one)
			w.WriteByte('\n')
		}
	}
}

func walker(path string, info os.FileInfo, _ error) error {
	// Prune out hidden directories and files, if not asked for
	name := info.Name()
	if !hidden && strings.HasPrefix(name, ".") {
		if info.IsDir() {
			return filepath.SkipDir
		}
		return nil
	}

	// If this is a file, process it
	if !info.IsDir() && isFileNameAcceptable(name) {
		processFile(path)
	}
	return nil
}

func isFileNameAcceptable(name string) bool {
	if len(extensions) == 0 {
		return true
	}
	if !caseSensitive {
		name = strings.ToLower(name)
	}
	for _, ext := range extensions {
		if strings.HasSuffix(name, ext) {
			return true
		}
	}
	return false
}

func processFile(path string) {
	// Compute the SHA-256 hash of the file contents
	f, err := os.Open(path)
	if err != nil {
		atomic.AddInt32(&filesUnableToProcess, 1)
		return
	}
	defer xio.CloseIgnoringErrors(f)
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		atomic.AddInt32(&filesUnableToProcess, 1)
		return
	}
	atomic.AddInt32(&filesProcessed, 1)
	atomic.AddInt64(&bytesProcessed, n)
	var sum [32]byte
	copy(sum[:], h.Sum(nil))

	// Add the info into our state
	needRemove := false
	lock.Lock()
	paths, exists := hashes[sum]
	if exists {
		atomic.AddInt32(&duplicatesFound, 1)
		atomic.AddInt64(&duplicateBytes, n)
		if remove {
			needRemove = true
		} else {
			hashes[sum] = append(paths, path)
		}
	} else {
		hashes[sum] = []string{path}
	}
	lock.Unlock()

	// Process any removal
	if needRemove {
		if removeOnlyFromLast && strings.HasPrefix(rel(removeOnlyFromLastRoot, path), "..") {
			return
		}
		if err = os.Remove(path); err != nil {
			lock.Lock()
			unableToRemove = append(unableToRemove, path)
			lock.Unlock()
		} else {
			lock.Lock()
			removed = append(removed, path)
			lock.Unlock()
		}
	}
}

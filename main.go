package main

import (
	"crypto/sha256"
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
	"github.com/richardwilkes/toolbox/atexit"
	"github.com/richardwilkes/toolbox/cmdline"
	"github.com/richardwilkes/toolbox/i18n"
	"github.com/richardwilkes/toolbox/txt"
	"github.com/richardwilkes/toolbox/xio"
	"github.com/richardwilkes/toolbox/xio/term"
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
	ansi                   *term.ANSI
	lock                   sync.Mutex
	hashes                 = make(map[[32]byte][]string)
	removed                []string
	unableToRemove         []string
	removeOnlyFromLastRoot string
)

func main() {
	cmdline.AppName = "Find Duplicates"
	cmdline.AppVersion = "1.0.2"
	cmdline.CopyrightHolder = "Richard Wilkes"
	cmdline.CopyrightYears = "2018-2022"
	cmdline.License = "Mozilla Public License Version 2.0"
	cl := cmdline.New(true)
	cl.UsageSuffix = "dirs..."
	cl.NewStringArrayOption(&extensions).SetName("extension").SetSingle('e').SetName("EXTENSION").SetUsage(i18n.Text("Limit processing to just files with the specified extension. May be specified more than once"))
	cl.NewBoolOption(&hidden).SetName("hidden").SetSingle('H').SetUsage(i18n.Text("Process files and directories that start with a period. These 'hidden' files are ignored by default"))
	cl.NewBoolOption(&remove).SetName("delete").SetSingle('d').SetUsage(i18n.Text("Delete all duplicates found. The first copy encountered will be preserved"))
	cl.NewBoolOption(&removeOnlyFromLast).SetName("last").SetSingle('l').SetUsage(i18n.Text("When deleting duplicates, only delete those found within the last directory tree specified on the command line"))
	cl.NewBoolOption(&caseSensitive).SetName("case").SetSingle('c').SetUsage(i18n.Text("Extensions are case-sensitive"))
	paths := cl.Parse(os.Args[1:])

	// If no paths specified, use the current directory
	if len(paths) == 0 {
		wd, err := os.Getwd()
		if err != nil {
			fmt.Println(i18n.Text("Unable to determine current working directory."))
			atexit.Exit(1)
		}
		paths = append(paths, wd)
	}

	// Determine the actual root paths and prune out paths that are a subset
	// of another
	set := make(map[string]int)
	order := 0
	for _, path := range paths {
		actual, err := realpath.Realpath(path)
		if err != nil {
			fmt.Printf(i18n.Text("Unable to determine real path for '%s'.\n"), path)
			atexit.Exit(1)
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
	ansi = term.NewANSI(os.Stdout)
	ansi.Clear()
	ansi.HideCursor()
	atexit.Register(func() {
		ansi.ShowCursor()
	})
	status()
	done := make(chan chan bool)
	go progress(done)

	// Ensure extensions are properly formatted
	var ext []string
	for _, one := range extensions {
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
		if err := filepath.Walk(one.path, walker); err != nil {
			fmt.Println(err)
			atexit.Exit(1)
		}
	}
	waitDone := make(chan bool)
	done <- waitDone
	<-waitDone

	// Report
	status()
	if remove {
		summarizeList(i18n.Text("Removed 1 file:"), i18n.Text("Removed %s files:"), removed)
		summarizeList(i18n.Text("Unable to remove 1 file:"), i18n.Text("Unable to remove %s files:"), unableToRemove)
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
				fmt.Println()
				fmt.Println(dup)
				for _, one := range m[dup] {
					fmt.Println(one)
				}
			}
		} else {
			fmt.Println()
			fmt.Println(i18n.Text("No duplicates found."))
		}
	}
}

func rel(base, target string) string {
	path, err := filepath.Rel(base, target)
	if err != nil {
		fmt.Println(err)
		atexit.Exit(1)
	}
	return path
}

func progress(done chan chan bool) {
	for {
		select {
		case response := <-done:
			ansi.ShowCursor()
			response <- true
			return
		case <-time.After(time.Second / 4):
			status()
		}
	}
}

func status() {
	count := atomic.LoadInt32(&filesProcessed)
	bytes := atomic.LoadInt64(&bytesProcessed)
	ansi.Position(1, 1)
	ansi.EraseLine()
	ansi.Foreground(term.White, term.Normal)
	fmt.Print(i18n.Text("Examined "))
	ansi.Foreground(term.Yellow, term.Bold)
	fmt.Print(humanize.Comma(int64(count)))
	ansi.Foreground(term.White, term.Normal)
	if count == 1 {
		fmt.Print(i18n.Text(" file, containing "))
	} else {
		fmt.Print(i18n.Text(" files, containing "))
	}
	ansi.Foreground(term.Yellow, term.Bold)
	fmt.Print(humanize.Comma(bytes))
	ansi.Foreground(term.White, term.Normal)
	if bytes == 1 {
		fmt.Println(i18n.Text(" byte."))
	} else {
		fmt.Println(i18n.Text(" bytes."))
	}

	count = atomic.LoadInt32(&duplicatesFound)
	bytes = atomic.LoadInt64(&duplicateBytes)
	ansi.EraseLine()
	fmt.Print(i18n.Text("Found "))
	ansi.Foreground(term.Yellow, term.Bold)
	fmt.Print(humanize.Comma(int64(count)))
	ansi.Foreground(term.White, term.Normal)
	if count == 1 {
		fmt.Print(i18n.Text(" duplicate file, containing "))
	} else {
		fmt.Print(i18n.Text(" duplicate files, containing "))
	}
	ansi.Foreground(term.Yellow, term.Bold)
	fmt.Print(humanize.Comma(bytes))
	ansi.Foreground(term.White, term.Normal)
	if bytes == 1 {
		fmt.Println(i18n.Text(" byte."))
	} else {
		fmt.Println(i18n.Text(" bytes."))
	}
}

func summarizeList(msgSingle, msgMultiple string, list []string) {
	if len(list) > 0 {
		sort.Slice(list, func(i, j int) bool { return txt.NaturalLess(list[i], list[j], true) })
		fmt.Println()
		if len(list) > 1 {
			fmt.Printf(msgMultiple, humanize.Comma(int64(len(list))))
			fmt.Println()
		} else {
			fmt.Println(msgSingle)
		}
		for _, one := range list {
			fmt.Println(one)
		}
	}
}

func walker(path string, info os.FileInfo, err error) error {
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

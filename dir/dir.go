package dir

import (
	"path"
	"strings"
)

func Clean(dir string) string {
	dir = path.Clean(dir)
	if !strings.HasSuffix(dir, "/") {
		dir += "/"
	}
	return dir
}

func Abs(dir string) string {
	if !strings.HasPrefix(dir, "/") {
		dir = "/" + dir
	}
	return Clean(dir)
}

func Branches(dir string) []string {
	dir = strings.Trim(dir, "/")
	return strings.Split(dir, "/")
}

func Join(elems ...string) string {
	dir := path.Join(elems...)
	return Clean(dir)
}

func IsAbs(dir string) bool {
	return path.IsAbs(dir)
}

func IsRoot(dir string) bool {
	dir = Clean(dir)
	return dir == "/"
}

func Depth(dir string) int {
	dir = Clean(dir)
	if IsRoot(dir) {
		return 0
	}

	dir = strings.Trim(dir, "/")
	return strings.Count(dir, "/") + 1
}

func ParentD(dir string, d int) string {
	dir = Clean(dir)
	branches := Branches(dir)
	if d >= len(branches) {
		return dir
	}

	pdir := Join(branches[:d]...)
	if IsAbs(dir) {
		return Abs(pdir)
	}
	return pdir
}

func SubD(dir string, d int) string {
	dir = Clean(dir)
	if IsAbs(dir) {
		dir = strings.TrimPrefix(dir, "/")
	}
	brances := Branches(dir)
	if d >= len(brances) {
		return dir
	}

	start := len(brances) - d
	sdir := Join(brances[start:]...)
	return sdir
}

func CParent(dir1, dir2 string) string {
	dir1 = Clean(dir1)
	dir2 = Clean(dir2)

	if IsAbs(dir1) {
		if !IsAbs(dir2) {
			return ""
		}
	} else {
		if IsAbs(dir2) {
			return ""
		}
	}

	bs1 := Branches(dir1)
	bs2 := Branches(dir2)
	minLen := len(bs1)
	if len(bs1) > len(bs2) {
		minLen = len(bs2)
	}

	i := 0
	for ; i < minLen; i++ {
		if bs1[i] != bs2[i] {
			break
		}
	}

	eqp := Join(bs1[:i]...)
	if IsAbs(dir1) {
		return Clean(Abs(eqp))
	}
	return eqp
}

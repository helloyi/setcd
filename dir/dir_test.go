package dir

import (
	"reflect"
	"testing"
)

func TestClean(t *testing.T) {
	type args struct {
		dir string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "relative dir1",
			args: args{
				dir: "a/b/c",
			},
			want: "a/b/c/",
		},
		{
			name: "relative dir2",
			args: args{
				dir: "a/b/c/",
			},
			want: "a/b/c/",
		},
		{
			name: "relative dir3",
			args: args{
				dir: "a/b/c/.",
			},
			want: "a/b/c/",
		},
		{
			name: "relative dir4",
			args: args{
				dir: "a/b/c/..",
			},
			want: "a/b/",
		},
		{
			name: "relative dir5",
			args: args{
				dir: "a/b/c/../../../",
			},
			want: "./",
		},
		{
			name: "relative dir6",
			args: args{
				dir: "a/b/c/../../../../",
			},
			want: "../",
		},
		{
			name: "absolute dir1",
			args: args{
				dir: "/a/b/c",
			},
			want: "/a/b/c/",
		},
		{
			name: "absolute dir2",
			args: args{
				dir: "/a/b/c/../../../../../",
			},
			want: "/",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Clean(tt.args.dir); got != tt.want {
				t.Errorf("Clean() = %v, want %v", got, tt.want)
			}
		})
	}
}

// func TestDir(t *testing.T) {
// 	type args struct {
// 		dir string
// 	}
// 	tests := []struct {
// 		name string
// 		args args
// 		want string
// 	}{
// 		{
// 			name: "relative dir",
// 			args: args{
// 				dir: "a/b/c/..",
// 			},
// 			want: "a/b/c/../",
// 		},
// 		{
// 			name: "absolute dir",
// 			args: args{
// 				dir: "/a/b/c/../",
// 			},
// 			want: "/a/b/c/../",
// 		},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			if got := Dir(tt.args.dir); got != tt.want {
// 				t.Errorf("Dir() = %v, want %v", got, tt.want)
// 			}
// 		})
// 	}
// }

func TestAbs(t *testing.T) {
	type args struct {
		dir string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "relative dir",
			args: args{
				dir: "a/b/c",
			},
			want: "/a/b/c/",
		},
		{
			name: "absolute dir",
			args: args{
				dir: "/a/b/c/../",
			},
			want: "/a/b/",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Abs(tt.args.dir); got != tt.want {
				t.Errorf("Abs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBranches(t *testing.T) {
	type args struct {
		dir string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "relative dir",
			args: args{
				dir: "a/b/c/..",
			},
			want: []string{"a", "b", "c", ".."},
		},
		{
			name: "absolute dir1",
			args: args{
				dir: "/a/b/c/..",
			},
			want: []string{"a", "b", "c", ".."},
		},
		{
			name: "absolute dir2",
			args: args{
				dir: "/a/b/c/../",
			},
			want: []string{"a", "b", "c", ".."},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Branches(tt.args.dir); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Branches() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJoin(t *testing.T) {
	type args struct {
		elems []string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "join 1",
			args: args{
				elems: []string{"a", "b", "c", ".."},
			},
			want: "a/b/",
		},
		{
			name: "join 2",
			args: args{
				elems: []string{"a", "b", "c", "."},
			},
			want: "a/b/c/",
		},
		{
			name: "join 3",
			args: args{
				elems: []string{"/a", "/b/", "c/"},
			},
			want: "/a/b/c/",
		},
		{
			name: "join 4",
			args: args{
				elems: []string{"/", "a", "/b/", "c/"},
			},
			want: "/a/b/c/",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Join(tt.args.elems...); got != tt.want {
				t.Errorf("Join() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsAbs(t *testing.T) {
	type args struct {
		dir string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "is abs 1",
			args: args{
				dir: "a/b/c/",
			},
			want: false,
		},
		{
			name: "is abs 2",
			args: args{
				dir: "/a/b/c/",
			},
			want: true,
		},
		{
			name: "is abs 3",
			args: args{
				dir: "/a/../../",
			},
			want: true,
		},
		{
			name: "is abs 4",
			args: args{
				dir: "a/../../",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsAbs(tt.args.dir); got != tt.want {
				t.Errorf("IsAbs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsRoot(t *testing.T) {
	type args struct {
		dir string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "is root 1",
			args: args{
				dir: "a/../../",
			},
			want: false,
		},
		{
			name: "is root 2",
			args: args{
				dir: "a/",
			},
			want: false,
		},
		{
			name: "is root 3",
			args: args{
				dir: "/",
			},
			want: true,
		},
		{
			name: "is root 4",
			args: args{
				dir: "/a/../../",
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsRoot(tt.args.dir); got != tt.want {
				t.Errorf("IsRoot() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDepth(t *testing.T) {
	type args struct {
		dir string
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "depth 1",
			args: args{
				dir: "/",
			},
			want: 0,
		},
		{
			name: "depth 2",
			args: args{
				dir: "/a/b",
			},
			want: 2,
		},
		{
			name: "depth 3",
			args: args{
				dir: "/a/b/../../../",
			},
			want: 0,
		},
		{
			name: "depth 4",
			args: args{
				dir: "a/b/../../../",
			},
			want: 1,
		},
		{
			name: "depth 5",
			args: args{
				dir: "a/b/../c/.",
			},
			want: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Depth(tt.args.dir); got != tt.want {
				t.Errorf("Depth() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParentD(t *testing.T) {
	type args struct {
		dir string
		d   int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "parent with depth 1",
			args: args{
				dir: "a/b/c",
				d:   1,
			},
			want: "a/",
		},
		{
			name: "parent with depth 2",
			args: args{
				dir: "/a/b/c",
				d:   2,
			},
			want: "/a/b/",
		},
		{
			name: "parent with depth 3",
			args: args{
				dir: "/a/b",
				d:   5,
			},
			want: "/a/b/",
		},
		{
			name: "parent with depth 4",
			args: args{
				dir: "/a/b/../",
				d:   2,
			},
			want: "/a/",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ParentD(tt.args.dir, tt.args.d); got != tt.want {
				t.Errorf("ParentD() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSubD(t *testing.T) {
	type args struct {
		dir string
		d   int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "sub with depth 1",
			args: args{
				dir: "a/b/c",
				d:   1,
			},
			want: "c/",
		},
		{
			name: "sub with depth 2",
			args: args{
				dir: "/a/b/c",
				d:   2,
			},
			want: "b/c/",
		},
		{
			name: "parent with depth 3",
			args: args{
				dir: "/a/b",
				d:   5,
			},
			want: "a/b/",
		},
		{
			name: "parent with depth 4",
			args: args{
				dir: "/a/b/../",
				d:   2,
			},
			want: "a/",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SubD(tt.args.dir, tt.args.d); got != tt.want {
				t.Errorf("SubD() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCParent(t *testing.T) {
	type args struct {
		dir1 string
		dir2 string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "common parent 1",
			args: args{
				dir1: "/a/b/c",
				dir2: "/a/b/d",
			},
			want: "/a/b/",
		},
		{
			name: "common parent 2",
			args: args{
				dir1: "a/b/c",
				dir2: "/a/b/d",
			},
			want: "",
		},
		{
			name: "common parent 3",
			args: args{
				dir1: "/a/b/c",
				dir2: "a/b/d",
			},
			want: "",
		},
		{
			name: "common parent 3",
			args: args{
				dir1: "a/b/c",
				dir2: "a/b/d",
			},
			want: "a/b/",
		},
		{
			name: "common parent 4",
			args: args{
				dir1: "a/../c",
				dir2: "a/../d",
			},
			want: "./",
		},
		{
			name: "common parent 5",
			args: args{
				dir1: "/a/../c",
				dir2: "/a/../d",
			},
			want: "/",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CParent(tt.args.dir1, tt.args.dir2); got != tt.want {
				t.Errorf("CParent() = %v, want %v", got, tt.want)
			}
		})
	}
}

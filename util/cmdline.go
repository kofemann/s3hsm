/* vim: set tabstop=2 softtabstop=2 shiftwidth=2 noexpandtab : */
package util

import (
	"strings"
)

func Options2Map(args []string) map[string]string {

	m := make(map[string]string)

	for i := 0; i < len(args); i++ {
		s := args[i]
		if s[0] != '-' {
			continue
		}

		n := strings.IndexByte(s, '=')
		if n != -1 {
			m[s[1:n]] = s[n+1:]
		} else {
			m[s[1:]] = ""
		}
	}

	return m
}

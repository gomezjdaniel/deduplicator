package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type noopOutput struct{}

func (o noopOutput) WriteString(_ string) (int, error) { return 0, nil }

func TestDeduplicatorLimitClients(t *testing.T) {
	t.Parallel()

	tcs := []struct {
		maxClients int
	}{
		{
			maxClients: 1,
		},
		{
			maxClients: 2,
		},
	}

	for _, tc := range tcs {
		t.Run(fmt.Sprintf("%d", tc.maxClients), func(t *testing.T) {
			dedup, err := newDeduplicator(":0", tc.maxClients, noopOutput{}, time.Second*10)
			require.Nil(t, err)

			var wg sync.WaitGroup
			wg.Add(1)

			go func(t *testing.T, wg *sync.WaitGroup) {
				defer wg.Done()
				err := dedup.start()
				require.Nil(t, err)
			}(t, &wg)

			time.Sleep(time.Second)

			for i := 1; i <= tc.maxClients+1; i++ {
				conn, err := net.Dial("tcp", dedup.ln.Addr().String())
				require.Nil(t, err)

				time.Sleep(time.Second)

				require.Nil(t, conn.SetReadDeadline(time.Now().Add(time.Second)))

				buf := make([]byte, 1)
				_, err = conn.Read(buf)
				if i <= tc.maxClients {
					assert.True(t, isTimeoutErr(err))
				} else {
					assert.Equal(t, io.EOF, err)
				}
			}

			dedup.cancel()

			wg.Wait()
		})
	}
}

type inMemoryOutput struct {
	output string
}

func (o *inMemoryOutput) WriteString(s string) (int, error) {
	o.output += s
	return len(s), nil
}

func (o inMemoryOutput) SortedOutput() (string, error) {
	if o.output == "" {
		return "", nil
	}

	nums := make([]int, 0)

	scanner := bufio.NewScanner(strings.NewReader(o.output))
	for scanner.Scan() {
		n, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return "", err
		}
		nums = append(nums, n)
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}

	sort.Ints(nums)

	sorted := ""
	for _, num := range nums {
		sorted += fmt.Sprintf("%09d\n", num)
	}

	return sorted, nil
}

func TestDeduplicator(t *testing.T) {
	t.Parallel()

	tcs := []struct {
		name                  string
		input                 [][]string
		expectedSortedOutput  string
		expectedNewUnique     int
		expectedNewDuplicated int
	}{
		{
			name: "normal input",
			input: [][]string{
				[]string{"000000001\n", "000001337\n"},
				[]string{"000000001\n", "000000001\n", "000000002\n", "000000001\n", "terminate\n"},
			},
			expectedSortedOutput: `000000001
000000002
000001337
`,
			expectedNewUnique:     3,
			expectedNewDuplicated: 3,
		},
		{
			name: "premature terminate",
			input: [][]string{
				[]string{"terminate\n", "000000002\n", "000000003\n"},
				[]string{"000000001\n", "000000001\n", "000000001\n", "terminate\n"},
			},
			expectedSortedOutput:  ``,
			expectedNewUnique:     0,
			expectedNewDuplicated: 0,
		},
		{
			name: "intersected terminate",
			input: [][]string{
				[]string{"000000001\n", "terminate\n"},
				[]string{"000000001\n", "000000001\n", "000000001\n", "000000002\n", "terminate\n"},
			},
			expectedSortedOutput: `000000001
`,
			expectedNewUnique:     1,
			expectedNewDuplicated: 0,
		},
		{
			name: "invalid input",
			input: [][]string{
				[]string{"abcABC\n", "111222333\n", "444555666\n"},
				[]string{"000000001\n", "000000001\n", "000000002\n", "terminate\n"},
			},
			expectedSortedOutput: `000000001
000000002
`,
			expectedNewUnique:     2,
			expectedNewDuplicated: 1,
		},
		{
			name: "invalid input",
			input: [][]string{
				[]string{"8\n", "111222333\n", "444555666\n"},
				[]string{"000000001\n", "000000001\n", "000000002\n", "terminate\n"},
			},
			expectedSortedOutput: `000000001
000000002
`,
			expectedNewUnique:     2,
			expectedNewDuplicated: 1,
		},
		{
			name: "invalid input",
			input: [][]string{
				[]string{"123456789ABC\n", "111222333\n", "444555666\n"},
				[]string{"000000001\n", "000000001\n", "000000002\n", "terminate\n"},
			},
			expectedSortedOutput: `000000001
000000002
`,
			expectedNewUnique:     2,
			expectedNewDuplicated: 1,
		},
		{
			name: "invalid input",
			input: [][]string{
				[]string{"000000005\n", "111222333", "444555666\n"},
				[]string{"000000001\n", "000000001\n", "000000002\n", "terminate\n"},
			},
			expectedSortedOutput: `000000001
000000002
000000005
`,
			expectedNewUnique:     3,
			expectedNewDuplicated: 1,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			output := new(inMemoryOutput)
			dedup, err := newDeduplicator(":0", len(tc.input), output, time.Second*10)
			require.Nil(t, err)

			var wg sync.WaitGroup
			wg.Add(1)

			go func(t *testing.T, wg *sync.WaitGroup) {
				defer wg.Done()
				err := dedup.start()
				require.Nil(t, err)
			}(t, &wg)

			time.Sleep(time.Second)

			assert.True(t, dedup.ready)

			var cwg sync.WaitGroup

			for i := 0; i < len(tc.input); i++ {
				cwg.Add(1)
				go func(cwg *sync.WaitGroup, i int) {
					defer cwg.Done()
					conn, err := net.Dial("tcp", dedup.ln.Addr().String())
					require.Nil(t, err)

					for _, input := range tc.input[i] {
						_, err = conn.Write([]byte(input))
						if err != nil && !isTemporaryErr(err) {
							return // server closed client's connection
						}
						assert.Nil(t, err)
					}
				}(&cwg, i)

				time.Sleep(time.Millisecond * 500)
			}

			cwg.Wait()

			time.Sleep(time.Second)

			sortedOutput, err := output.SortedOutput()
			assert.Nil(t, err)
			assert.Equal(t, tc.expectedSortedOutput, sortedOutput)
			assert.Equal(t, tc.expectedNewUnique, int(dedup.newUnique))
			assert.Equal(t, tc.expectedNewDuplicated, int(dedup.newDuplicated))

			wg.Wait()

			assert.False(t, dedup.ready)
		})
	}
}

package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"
)

func main() {
	// 1. Define CLI Flags
	inputFile := flag.String("in", "", "Path to the input SQL file")
	outputFile := flag.String("out", "", "Path to the output SQL file")
	skipTables := flag.String("skip-tables", "", "Comma-separated list of tables to remove (DDL and Data)")
	flag.Parse()

	if *inputFile == "" || *outputFile == "" {
		fmt.Println("Error: Both input and output files are required.")
		fmt.Println("Usage: sqlcleaner -in dump.sql -out clean_dump.sql -skip-tables users,logs,cache")
		os.Exit(1)
	}

	// 2. Parse tables to skip into a map for O(1) lookups
	tableMap := make(map[string]bool)
	for _, t := range strings.Split(*skipTables, ",") {
		t = strings.TrimSpace(t)
		if t != "" {
			tableMap[t] = true
		}
	}

	// 3. Open Input and Output files
	in, err := os.Open(*inputFile)
	if err != nil {
		fmt.Printf("Error opening input file: %v\n", err)
		os.Exit(1)
	}
	defer in.Close()

	out, err := os.Create(*outputFile)
	if err != nil {
		fmt.Printf("Error creating output file: %v\n", err)
		os.Exit(1)
	}
	defer out.Close()

	// 4. Compile Regex Patterns
	// Matches `CREATE TABLE foo`, `DROP TABLE foo`, `INSERT INTO foo`, `LOCK TABLES foo`
	reTable := regexp.MustCompile(`(?i)^(?:CREATE\s+TABLE|DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?|INSERT\s+INTO|LOCK\s+TABLES)\s+(?:IF NOT EXISTS\s+)?\` + "`" + `?([a-zA-Z0-9_]+)\` + "`" + `?`)

	// Matches dummy tables created for views by mysqldump
	reDummyView := regexp.MustCompile(`(?i)^--\s+Temporary\s+view\s+structure\s+for\s+view\s+\` + "`" + `?([a-zA-Z0-9_]+)\` + "`" + `?`)

	// Matches View DDL (including mysqldump's comment-wrapped syntax)
	reCreateView := regexp.MustCompile(`(?i)^(?:/\*\!\d+\s+)?CREATE\s+(?:ALGORITHM|DEFINER|SQL SECURITY|VIEW)`)
	reDropView := regexp.MustCompile(`(?i)^(?:/\*\!\d+\s+)?DROP\s+VIEW`)

	// Matches Charset and Collation definitions
	reCharset := regexp.MustCompile(`(?i)(?:DEFAULT\s+)?(?:CHARACTER\s+SET|CHARSET)\s*=?\s*[a-zA-Z0-9_]+`)
	reCollate := regexp.MustCompile(`(?i)COLLATE\s*=?\s*[a-zA-Z0-9_]+`)

	// 5. Configure the Scanner
	scanner := bufio.NewScanner(in)
	// Allocate a 100MB buffer to prevent "bufio.Scanner: token too long" on huge extended inserts
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 100*1024*1024)

	// State Tracking
	skippingStatement := false
	skippingTableData := false

	for scanner.Scan() {
		line := scanner.Text()
		trimmedLine := strings.TrimSpace(line)
		upperLine := strings.ToUpper(trimmedLine)

		// --- Rule 1: Remove GTID ---
		if strings.Contains(upperLine, "GTID_PURGED") {
			if !strings.HasSuffix(trimmedLine, ";") {
				skippingStatement = true
			}
			continue
		}

		// --- State Machine Resolution ---
		// If we are currently skipping a multi-line statement, wait until we see the semicolon
		if skippingStatement {
			if strings.HasSuffix(trimmedLine, ";") {
				skippingStatement = false
			}
			continue
		}

		// If we are inside a `LOCK TABLES ... WRITE` block, skip until we hit `UNLOCK TABLES`
		if skippingTableData {
			if strings.HasPrefix(upperLine, "UNLOCK TABLES") {
				skippingTableData = false
			}
			continue 
		}

		// --- Rule 2: Remove Views (and their dummy tables) ---
		// mysqldump creates dummy tables before views. We dynamically add the view name 
		// to our ignore map so the dummy table DDL gets stripped automatically.
		if match := reDummyView.FindStringSubmatch(trimmedLine); len(match) > 1 {
			tableMap[match[1]] = true
			continue
		}
		if reDropView.MatchString(trimmedLine) || reCreateView.MatchString(trimmedLine) {
			if !strings.HasSuffix(trimmedLine, ";") {
				skippingStatement = true
			}
			continue
		}

		// --- Rule 3: Remove Specific Table DDL and Data ---
		if match := reTable.FindStringSubmatch(trimmedLine); len(match) > 1 {
			tableName := match[1]
			if tableMap[tableName] {
				if strings.HasPrefix(upperLine, "LOCK TABLES") {
					skippingTableData = true
					continue
				}
				if !strings.HasSuffix(trimmedLine, ";") {
					skippingStatement = true
				}
				continue
			}
		}

		// --- Rule 4: Remove Charset and Collate ---
		cleanedLine := reCharset.ReplaceAllString(line, "")
		cleanedLine = reCollate.ReplaceAllString(cleanedLine, "")
		
		// Clean up dangling spaces left over before semicolons or commas
		cleanedLine = strings.ReplaceAll(cleanedLine, " ;", ";")
		cleanedLine = strings.ReplaceAll(cleanedLine, " ,", ",")

		// Write the processed line to the new file
		out.WriteString(cleanedLine + "\n")
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Critical error while reading input file: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("SQL file successfully cleaned!")
}
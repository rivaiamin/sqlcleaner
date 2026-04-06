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

	// Matches View DDL (must include VIEW — not CREATE DEFINER=… PROCEDURE)
	reCreateView := regexp.MustCompile(`(?i)^(?:/\*\!\d+\s+)?CREATE\s+(?:OR\s+REPLACE\s+)?(?:ALGORITHM\s*=\s*\S+\s+|DEFINER\s*=\s*\S+\s+|SQL\s+SECURITY\s+\S+\s+)*VIEW\s+`)
	reDropView := regexp.MustCompile(`(?i)^(?:/\*\!\d+\s+)?DROP\s+VIEW`)

	// Matches Charset and Collation definitions
	reCharset := regexp.MustCompile(`(?i)(?:DEFAULT\s+)?(?:CHARACTER\s+SET|CHARSET)\s*=?\s*[a-zA-Z0-9_]+`)
	reCollate := regexp.MustCompile(`(?i)COLLATE\s*=?\s*[a-zA-Z0-9_]+`)

	// DELIMITER <token> (mysql client; mysqldump uses this around routines)
	reDelimiter := regexp.MustCompile(`(?i)^DELIMITER\s+(.+)$`)

	// CREATE PROCEDURE / FUNCTION (with optional mysqldump /*!...*/ prefixes)
	reCreateRoutine := regexp.MustCompile(`(?i)^(?:/\*!\d+\s+)?CREATE\s+(?:OR\s+REPLACE\s+)?(?:DEFINER\s*=\s*\S+\s+|SQL\s+SECURITY\s+(?:DEFINER|INVOKER)\s+)*(PROCEDURE|FUNCTION)\s+`)
	reCreateRoutineSplit := regexp.MustCompile(`(?i)/\*!\d+\s+CREATE\s*\*/\s*/\*!\d+\s+(?:OR\s+REPLACE\s+)?(?:DEFINER\s*=\s*\S+\s+|SQL\s+SECURITY\s+(?:DEFINER|INVOKER)\s+)*(PROCEDURE|FUNCTION)\s+`)
	reCreateRoutineOneCmt := regexp.MustCompile(`(?i)^/\*!\d+\s+CREATE\s+(?:OR\s+REPLACE\s+)?(?:DEFINER\s*=\s*\S+\s+|SQL\s+SECURITY\s+(?:DEFINER|INVOKER)\s+)*(PROCEDURE|FUNCTION)\s+`)
	// Trailing mysqldump versioned comment before routine terminator (e.g. "END */;;")
	reTrailVersCmt := regexp.MustCompile(`(?i)\s*/\*![0-9]+\s+.*?\*/\s*$`)

	// 5. Configure the Scanner
	scanner := bufio.NewScanner(in)
	// Allocate a 100MB buffer to prevent "bufio.Scanner: token too long" on huge extended inserts
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 100*1024*1024)

	// State Tracking
	skippingStatement := false
	skipStmtTerminator := ";"
	skippingIsRoutine := false
	skippingTableData := false
	// Default SQL statement terminator; overridden by DELIMITER until next DELIMITER
	stmtDelim := ";"
	// Buffered "DELIMITER …" to emit before the next non-empty kept line (mysqldump may insert blanks/comments)
	pendingDelimLine := ""
	// After stripping a routine, swallow the following "DELIMITER ;" that mysqldump uses to reset the client
	afterDroppedRoutine := false

	flushPendingDelim := func() {
		if pendingDelimLine != "" {
			out.WriteString(pendingDelimLine + "\n")
			pendingDelimLine = ""
		}
	}

	for scanner.Scan() {
		line := scanner.Text()
		trimmedLine := strings.TrimSpace(line)
		upperLine := strings.ToUpper(trimmedLine)

		// --- Rule 1: Remove GTID ---
		if strings.Contains(upperLine, "GTID_PURGED") {
			if !strings.HasSuffix(trimmedLine, ";") {
				skippingStatement = true
				skipStmtTerminator = ";"
				skippingIsRoutine = false
			}
			continue
		}

		// --- State Machine Resolution ---
		// If we are currently skipping a multi-line statement, wait until the line ends with the active terminator
		if skippingStatement {
			done := strings.HasSuffix(trimmedLine, skipStmtTerminator)
			if done && skippingIsRoutine {
				// With DELIMITER ;;, inner statements must not use ";;" as EOL or we'd stop early.
				// Only the final "END [comment] ;;" closes the routine.
				rest := strings.TrimSpace(strings.TrimSuffix(trimmedLine, skipStmtTerminator))
				rest = reTrailVersCmt.ReplaceAllString(rest, "")
				rest = strings.TrimSpace(rest)
				done = strings.EqualFold(rest, "END")
			}
			if done {
				skippingStatement = false
				if skippingIsRoutine {
					afterDroppedRoutine = true
					skippingIsRoutine = false
				}
				stmtDelim = ";"
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

		// --- Rule 2b: DELIMITER — track stmt terminator; emit normalized "DELIMITER <token>" when keeping SQL ---
		if m := reDelimiter.FindStringSubmatch(trimmedLine); len(m) > 1 {
			tok := strings.TrimSpace(m[1])
			if tok == "" {
				tok = ";"
			}
			stmtDelim = tok
			if afterDroppedRoutine && tok == ";" {
				afterDroppedRoutine = false
				continue
			}
			afterDroppedRoutine = false
			pendingDelimLine = "DELIMITER " + tok
			continue
		}

		// --- Rule 2c: Remove stored procedures and functions (before VIEW matching — CREATE DEFINER also starts procedures) ---
		if reCreateRoutine.MatchString(trimmedLine) || reCreateRoutineSplit.MatchString(trimmedLine) || reCreateRoutineOneCmt.MatchString(trimmedLine) {
			pendingDelimLine = ""
			if strings.HasSuffix(trimmedLine, stmtDelim) {
				afterDroppedRoutine = true
				stmtDelim = ";"
			} else {
				skippingStatement = true
				skipStmtTerminator = stmtDelim
				skippingIsRoutine = true
			}
			continue
		}

		if reDropView.MatchString(trimmedLine) || reCreateView.MatchString(trimmedLine) {
			if !strings.HasSuffix(trimmedLine, ";") {
				skippingStatement = true
				skipStmtTerminator = ";"
				skippingIsRoutine = false
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
					skipStmtTerminator = ";"
					skippingIsRoutine = false
				}
				continue
			}
		}

		// --- Rule 4: Remove Charset and Collate ---
		if trimmedLine != "" {
			flushPendingDelim()
		}
		cleanedLine := reCharset.ReplaceAllString(line, "")
		cleanedLine = reCollate.ReplaceAllString(cleanedLine, "")

		// Clean up dangling spaces left over before semicolons or commas.
		// Do not apply to DELIMITER lines: "DELIMITER ;;" contains " ;" before the first semicolon and would become "DELIMITER;;".
		if !reDelimiter.MatchString(strings.TrimSpace(cleanedLine)) {
			cleanedLine = strings.ReplaceAll(cleanedLine, " ;", ";")
			cleanedLine = strings.ReplaceAll(cleanedLine, " ,", ",")
		}

		// Write the processed line to the new file
		out.WriteString(cleanedLine + "\n")
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Critical error while reading input file: %v\n", err)
		os.Exit(1)
	}

	flushPendingDelim()

	fmt.Println("SQL file successfully cleaned!")
}

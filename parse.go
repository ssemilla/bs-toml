package toml

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

type parser struct {
	mapping map[string]interface{}
	types   map[string]tomlType
	lx      *lexer

	// A list of keys in the order that they appear in the TOML data.
	ordered []Key

	// the full key for the current hash in scope
	context Key

	// the base key name for everything except hashes
	currentKey Key

	// rough approximation of line number
	approxLine int
}

type parseError string

func (pe parseError) Error() string {
	return string(pe)
}

func parse(data string) (p *parser, err error) {
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			if err, ok = r.(parseError); ok {
				return
			}
			panic(r)
		}
	}()

	p = &parser{
		mapping: make(map[string]interface{}),
		types:   make(map[string]tomlType),
		lx:      lex(data),
		ordered: make([]Key, 0),
	}
	for {
		item := p.next()
		if item.typ == itemEOF {
			break
		}
		p.topLevel(item)
	}

	return p, nil
}

func (p *parser) panicf(format string, v ...interface{}) {
	msg := fmt.Sprintf("Near line %d (last key parsed '%s'): %s",
		p.approxLine, p.current(), fmt.Sprintf(format, v...))
	panic(parseError(msg))
}

func (p *parser) next() item {
	it := p.lx.nextItem()
	if it.typ == itemError {
		p.panicf("%s", it.val)
	}
	return it
}

func (p *parser) bug(format string, v ...interface{}) {
	panic(fmt.Sprintf("BUG: "+format+"\n\n", v...))
}

func (p *parser) expect(typ itemType) item {
	it := p.next()
	p.assertEqual(typ, it.typ)
	return it
}

func (p *parser) assertEqual(expected, got itemType) {
	if expected != got {
		p.bug("Expected '%s' but got '%s'.", expected, got)
	}
}

func (p *parser) topLevel(item item) {
	switch item.typ {
	case itemCommentStart:
		p.approxLine = item.line
		p.expect(itemText)

	case itemTableStart:
		key := p.getKey(itemTableStart)
		p.expect(itemTableEnd)
		p.establishContext(key, false)
		p.setType(nil, tomlHash)
		p.ordered = append(p.ordered, key)

	case itemArrayTableStart:
		key := p.getKey(itemArrayTableStart)
		p.expect(itemArrayTableEnd)
		p.establishContext(key, true)
		p.setType(nil, tomlArrayHash)
		p.ordered = append(p.ordered, key)

	case itemKeyStart:
		key := p.getKey(itemKeyStart)
		p.currentKey = key
		val, typ := p.value(p.next())
		p.setValue(key, val)
		p.setType(key, typ)
		p.ordered = append(p.ordered, p.context.add(key...))
		p.currentKey = nil

	default:
		p.bug("Unexpected type at top level: %s", item.typ)
	}
}

// Gets a string for a key (or part of a key in a table name).
func (p *parser) keyString(it item) string {
	switch it.typ {
	case itemText:
		return it.val
	case itemString, itemMultilineString,
		itemRawString, itemRawMultilineString:
		s, _ := p.value(it)
		return s.(string)
	default:
		p.bug("Unexpected key type: %s", it.typ)
		panic("unreachable")
	}
}

func (p *parser) getKey(from itemType) Key {
	if from != itemKeyStart {
		p.expect(itemKeyStart)
	}
	kg := p.next()
	p.approxLine = kg.line
	var key Key
	for ; kg.typ != itemKeyEnd && kg.typ != itemEOF; kg = p.next() {
		key = append(key, p.keyString(kg))
	}
	p.assertEqual(itemKeyEnd, kg.typ)
	return key
}

// value translates an expected value from the lexer into a Go value wrapped
// as an empty interface.
func (p *parser) value(it item) (interface{}, tomlType) {
	switch it.typ {
	case itemString:
		return p.replaceEscapes(it.val), p.typeOfPrimitive(it)
	case itemMultilineString:
		trimmed := stripFirstNewline(stripEscapedWhitespace(it.val))
		return p.replaceEscapes(trimmed), p.typeOfPrimitive(it)
	case itemRawString:
		return it.val, p.typeOfPrimitive(it)
	case itemRawMultilineString:
		return stripFirstNewline(it.val), p.typeOfPrimitive(it)
	case itemBool:
		switch it.val {
		case "true":
			return true, p.typeOfPrimitive(it)
		case "false":
			return false, p.typeOfPrimitive(it)
		}
		p.bug("Expected boolean value, but got '%s'.", it.val)
	case itemInteger:
		if !numUnderscoresOK(it.val) {
			p.panicf("Invalid integer %q: underscores must be surrounded by digits",
				it.val)
		}
		val := strings.Replace(it.val, "_", "", -1)
		num, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			// Distinguish integer values. Normally, it'd be a bug if the lexer
			// provides an invalid integer, but it's possible that the number is
			// out of range of valid values (which the lexer cannot determine).
			// So mark the former as a bug but the latter as a legitimate user
			// error.
			if e, ok := err.(*strconv.NumError); ok &&
				e.Err == strconv.ErrRange {

				p.panicf("Integer '%s' is out of the range of 64-bit "+
					"signed integers.", it.val)
			} else {
				p.bug("Expected integer value, but got '%s'.", it.val)
			}
		}
		return num, p.typeOfPrimitive(it)
	case itemFloat:
		parts := strings.FieldsFunc(it.val, func(r rune) bool {
			switch r {
			case '.', 'e', 'E':
				return true
			}
			return false
		})
		for _, part := range parts {
			if !numUnderscoresOK(part) {
				p.panicf("Invalid float %q: underscores must be "+
					"surrounded by digits", it.val)
			}
		}
		if !numPeriodsOK(it.val) {
			// As a special case, numbers like '123.' or '1.e2',
			// which are valid as far as Go/strconv are concerned,
			// must be rejected because TOML says that a fractional
			// part consists of '.' followed by 1+ digits.
			p.panicf("Invalid float %q: '.' must be followed "+
				"by one or more digits", it.val)
		}
		val := strings.Replace(it.val, "_", "", -1)
		num, err := strconv.ParseFloat(val, 64)
		if err != nil {
			if e, ok := err.(*strconv.NumError); ok &&
				e.Err == strconv.ErrRange {

				p.panicf("Float '%s' is out of the range of 64-bit "+
					"IEEE-754 floating-point numbers.", it.val)
			} else {
				p.panicf("Invalid float value: %q", it.val)
			}
		}
		return num, p.typeOfPrimitive(it)
	case itemDatetime:
		var t time.Time
		var ok bool
		var err error
		for _, format := range []string{
			"2006-01-02T15:04:05Z07:00",
			"2006-01-02T15:04:05",
			"2006-01-02",
		} {
			t, err = time.ParseInLocation(format, it.val, time.Local)
			if err == nil {
				ok = true
				break
			}
		}
		if !ok {
			p.panicf("Invalid TOML Datetime: %q.", it.val)
		}
		return t, p.typeOfPrimitive(it)
	case itemArray:
		array := make([]interface{}, 0)
		types := make([]tomlType, 0)

		for it = p.next(); it.typ != itemArrayEnd; it = p.next() {
			if it.typ == itemCommentStart {
				p.expect(itemText)
				continue
			}

			val, typ := p.value(it)
			array = append(array, val)
			types = append(types, typ)
		}
		return array, p.typeOfArray(types)
	case itemInlineTableStart:
		var (
			hash         = make(map[string]interface{})
			outerContext = p.context
			outerKey     = p.currentKey
		)

		p.currentKey = nil
		for it := p.next(); it.typ != itemInlineTableEnd; it = p.next() {
			if it.typ != itemKeyStart {
				p.bug("Expected key start but instead found %q, around line %d",
					it.val, p.approxLine)
			}
			if it.typ == itemCommentStart {
				p.expect(itemText)
				continue
			}

			key := p.getKey(it.typ)
			p.currentKey = key
			p.context = outerContext.add(key...)
			val, typ := p.value(p.next())
			p.context = outerContext
			p.setInlineVal(hash, key, val)
			// make sure we keep metadata up to date
			p.setType(key, typ)

			p.ordered = append(p.ordered, p.context.add(key...))
			p.currentKey = outerKey
		}
		return hash, tomlHash
	}
	p.bug("Unexpected value type: %s", it.typ)
	panic("unreachable")
}

// numUnderscoresOK checks whether each underscore in s is surrounded by
// characters that are not underscores.
func numUnderscoresOK(s string) bool {
	accept := false
	for _, r := range s {
		if r == '_' {
			if !accept {
				return false
			}
			accept = false
			continue
		}
		accept = true
	}
	return accept
}

// numPeriodsOK checks whether every period in s is followed by a digit.
func numPeriodsOK(s string) bool {
	period := false
	for _, r := range s {
		if period && !isDigit(r) {
			return false
		}
		period = r == '.'
	}
	return !period
}

// establishContext sets the current context of the parser,
// where the context is either a hash or an array of hashes. Which one is
// set depends on the value of the `array` parameter.
func (p *parser) establishContext(key Key, array bool) {

	// Always start at the top level and drill down for our context.
	context, tail := key.splitTail()
	hash := p.mapping

	for _, k := range context {

		// No key? Make an implicit hash and move on.
		if _, ok := hash[k]; !ok {
			hash[k] = make(map[string]interface{})
		}

		// If the hash context is actually an array of tables, then set
		// the hash context to the last element in that array.
		//
		// Otherwise, it better be a table, since this MUST be a key group (by
		// virtue of it not being the last element in a key).
		switch t := hash[k].(type) {
		case []map[string]interface{}:
			hash = t[len(t)-1]
		case map[string]interface{}:
			hash = t
		default:
			p.panicf("Key '%s' was already created as a hash.", key)
		}
	}

	if array {
		// If this is the first element for this array, then allocate a new
		// list of tables for it.
		if _, ok := hash[tail]; !ok {
			hash[tail] = make([]map[string]interface{}, 0, 8)
		}

		// Add a new table. But make sure the key hasn't already been used
		// for something else.
		if arrayMap, ok := hash[tail].([]map[string]interface{}); ok {
			hash[tail] = append(arrayMap, make(map[string]interface{}))
		} else {
			p.panicf("Key '%s' was already created and cannot be used as "+
				"an array.", key)
		}
	} else {

		if _, ok := hash[tail]; !ok {
			hash[tail] = make(map[string]interface{})
		}

		if _, ok := hash[tail].(map[string]interface{}); !ok {
			p.panicf("Key '%s' was already created and cannot be used as "+
				"a hash.", key)
		}
	}

	p.context = key
}

// setValue sets the given key to the given value in the current context.
func (p *parser) setValue(key Key, value interface{}) {

	// traverse current context
	hash := p.mapping
	for _, k := range p.context {

		v, ok := hash[k]
		if !ok {
			p.bug("Context for key '%s' has not been established.", p.context)
		}

		switch t := v.(type) {
		case []map[string]interface{}:
			// The context is a table of hashes. Pick the most recent table
			// defined as the current hash.
			hash = t[len(t)-1]
		case map[string]interface{}:
			hash = t
		default:
			p.bug("Expected hash to have type 'map[string]interface{}', but "+
				"it has '%T' instead.", t)
		}
	}

	// traverse key
	keyContext, tail := key.splitTail()
	for _, k := range keyContext {

		if _, ok := hash[k]; !ok {
			hash[k] = make(map[string]interface{})
		}

		switch v := hash[k].(type) {
		case []map[string]interface{}:
			hash = v[len(v)-1]
		case map[string]interface{}:
			hash = v
		default:
			p.bug("Expected hash to have type 'map[string]interface{}, but "+
				"it has '%T' instead.", v)
		}

	}

	if _, ok := hash[tail]; ok {
		p.panicf("Duplicated key '%s.%s'", p.context, key)
	} else {
		hash[tail] = value
	}
}

// setInlineVal sets the value in an inline map.
func (p *parser) setInlineVal(hash map[string]interface{}, key Key, val interface{}) {

	context, tail := key.splitTail()

	for _, k := range context {

		if _, ok := hash[k]; !ok {
			hash[k] = make(map[string]interface{})
		}

		if next, ok := hash[k].(map[string]interface{}); ok {
			hash = next
		} else {
			p.bug("Key was already set to %v, around line %d", next, p.approxLine)
		}
	}

	if _, ok := hash[tail]; ok {
		p.bug("Duplicate key, around line %d", p.approxLine)
	}

	hash[tail] = val
}

// setType sets the type of a particular value at a given key.
// It should be called immediately AFTER setValue.
//
// Note that if `key` is empty, then the type given will be applied to the
// current context (which is either a table or an array of tables).
func (p *parser) setType(key Key, typ tomlType) {
	p.types[p.context.add(key...).String()] = typ
}

// current returns the full key name of the current context.
func (p *parser) current() string {
	if len(p.currentKey) == 0 {
		return p.context.String()
	}
	if len(p.context) == 0 {
		return p.currentKey.String()
	}
	return p.context.add(p.currentKey...).String()
}

func stripFirstNewline(s string) string {
	if len(s) == 0 || s[0] != '\n' {
		return s
	}
	return s[1:]
}

func stripEscapedWhitespace(s string) string {
	esc := strings.Split(s, "\\\n")
	if len(esc) > 1 {
		for i := 1; i < len(esc); i++ {
			esc[i] = strings.TrimLeftFunc(esc[i], unicode.IsSpace)
		}
	}
	return strings.Join(esc, "")
}

func (p *parser) replaceEscapes(str string) string {
	var replaced []rune
	s := []byte(str)
	r := 0
	for r < len(s) {
		if s[r] != '\\' {
			c, size := utf8.DecodeRune(s[r:])
			r += size
			replaced = append(replaced, c)
			continue
		}
		r += 1
		if r >= len(s) {
			p.bug("Escape sequence at end of string.")
			return ""
		}
		switch s[r] {
		default:
			p.bug("Expected valid escape code after \\, but got %q.", s[r])
			return ""
		case 'b':
			replaced = append(replaced, rune(0x0008))
			r += 1
		case 't':
			replaced = append(replaced, rune(0x0009))
			r += 1
		case 'n':
			replaced = append(replaced, rune(0x000A))
			r += 1
		case 'f':
			replaced = append(replaced, rune(0x000C))
			r += 1
		case 'r':
			replaced = append(replaced, rune(0x000D))
			r += 1
		case '"':
			replaced = append(replaced, rune(0x0022))
			r += 1
		case '\\':
			replaced = append(replaced, rune(0x005C))
			r += 1
		case 'u':
			// At this point, we know we have a Unicode escape of the form
			// `uXXXX` at [r, r+5). (Because the lexer guarantees this
			// for us.)
			escaped := p.asciiEscapeToUnicode(s[r+1 : r+5])
			replaced = append(replaced, escaped)
			r += 5
		case 'U':
			// At this point, we know we have a Unicode escape of the form
			// `uXXXX` at [r, r+9). (Because the lexer guarantees this
			// for us.)
			escaped := p.asciiEscapeToUnicode(s[r+1 : r+9])
			replaced = append(replaced, escaped)
			r += 9
		}
	}
	return string(replaced)
}

func (p *parser) asciiEscapeToUnicode(bs []byte) rune {
	s := string(bs)
	hex, err := strconv.ParseUint(strings.ToLower(s), 16, 32)
	if err != nil {
		p.bug("Could not parse '%s' as a hexadecimal number, but the "+
			"lexer claims it's OK: %s", s, err)
	}
	if !utf8.ValidRune(rune(hex)) {
		p.panicf("Escaped character '\\u%s' is not valid UTF-8.", s)
	}
	return rune(hex)
}

func isStringType(ty itemType) bool {
	return ty == itemString || ty == itemMultilineString ||
		ty == itemRawString || ty == itemRawMultilineString
}

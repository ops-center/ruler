package promquery

import (
	"fmt"
	"github.com/pkg/errors"
	"strings"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
)

// It will add label matchers to every metrics
// TODO: Add recover panic
func AddLabelMatchersToQuery(q string, labels []labels.Label) (newQ string, err error) {
	newQ = ""
	defer func() {
		if err2 := recover(); err2 != nil {
			err = errors.Errorf("Add labels matchers to query: %v", err2)
		}
	}()

	extraLbs := LabelsToString(labels)
	// all char are added to 'newQ' upto this position
	qPos := -1

	p := newParser(q)
	itm := p.next()
	for itm.typ != itemEOF {
		nextItm := p.next()
		if itm.typ == itemIdentifier || itm.typ == itemMetricIdentifier {
			if nextItm.typ != itemLeftParen { // to differential with function
				if nextItm.typ == itemLeftBrace { // http{} or http{method="get"}
					// copy all 'q' char from 'qPos+1' to 'nextItm.Pos'
					newQ = newQ + q[qPos+1:nextItm.pos+1]
					qPos = int(nextItm.pos)
					exists := false // already labels exists or not, e.g. http{}, http{method="get"}
					for nextItm.typ != itemRightBrace && nextItm.typ != itemEOF {
						nextItm = p.next()
						if nextItm.typ == itemString || nextItm.typ == itemIdentifier {
							exists = true
						}
					}

					newQ = newQ + extraLbs
					if exists {
						newQ = newQ + ","
					}
				} else { // http
					// copy all 'q' char from 'qPos+1' to 'itm.Pos + len(itm.val)'
					en := int(itm.pos) + len(itm.val)
					newQ = newQ + q[qPos+1:en] + fmt.Sprintf("{%s}", extraLbs)
					qPos = en - 1
				}
			}
		}

		if itm.typ == itemLeftBrace { // handle case: {__name__=~"job:.*"}
			exists := false // '__name__' exists
			for nextItm.typ != itemRightBrace && nextItm.typ != itemEOF {
				if nextItm.typ == itemIdentifier && nextItm.val == model.MetricNameLabel {
					exists = true
				}
				nextItm = p.next()
			}

			if exists {
				// copy all 'q' char from 'qPos+1' to 'itm.Pos'
				newQ = newQ + q[qPos+1:itm.pos+1]
				qPos = int(itm.pos)
				newQ = newQ + extraLbs + ","
			}
		}

		if itm.typ > keywordsStart && itm.typ < keywordsEnd {
			if nextItm.typ == itemLeftParen {
				for nextItm.typ != itemRightParen && nextItm.typ != itemEOF {
					nextItm = p.next()
				}
			}
		}

		itm = nextItm
	}

	if qPos + 1 < len(q) {
		newQ = newQ + q[qPos+1:]
	}

	newQ, err = RemoveDuplicateLabels(newQ, labels)
	return newQ, err
}

// Remove duplicate of these given lables
func RemoveDuplicateLabels(q string, labels []labels.Label) (newQ string, err error) {
	newQ = ""
	defer func() {
		if err2 := recover(); err2 != nil {
			err = errors.Errorf("remove duplicate labels matchers: %v", err2)
		}
	}()

	// all char are added to 'newQ' upto this position
	qPos := -1

	isLabelUsedMap := map[string]bool{}

	p := newParser(q)
	itm := p.next()
	for itm.typ != itemEOF {
		if itm.typ == itemLeftBrace {
			newQ = newQ + q[qPos+1:itm.pos+1]
			qPos = int(itm.pos)

			for _, lb := range labels {
				isLabelUsedMap[lb.Name] = false
			}

			p.backup()
			lMatchers := p.labelMatchers()

			for pos, lm := range lMatchers {

				// fmt.Println("-->", lm.String())

				if isUsed, found := isLabelUsedMap[lm.Name]; found {
					if !isUsed {
						isLabelUsedMap[lm.Name] = true
						if pos > 0 {
							newQ = newQ + ","
						}
						newQ = newQ + lm.String()
					}
				} else {
					if pos > 0 {
						newQ = newQ + ","
					}
					newQ = newQ + lm.String()
				}
				// fmt.Println("==>", newQ)
			}

			// p.labelMatchers() ensures that it ends with '}'
			newQ = newQ + "}"
			p.backup()
			itm = p.peek()
			qPos = int(itm.pos)
		} else {
			itm = p.next()
		}
	}

	if qPos + 1 < len(q) {
		newQ = newQ + q[qPos+1:]
	}
	return newQ, err
}

func LabelsToString(labels []labels.Label) string {
	lbs := []string{}
	for _, l := range labels {
		lbs = append(lbs, fmt.Sprintf(`%s="%s"`, l.Name, l.Value))
	}
	return strings.Join(lbs, ",")
}

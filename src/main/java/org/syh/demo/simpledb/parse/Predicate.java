package org.syh.demo.simpledb.parse;

import org.syh.demo.simpledb.query.Scan;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Predicate {
    private List<Term> terms;

    public Predicate() {
        this.terms = new ArrayList<>();
    }

    public Predicate(Term term) {
        this.terms = new ArrayList<>();
        this.terms.add(term);
    }

    public void conjoinWith(Predicate predicate) {
        this.terms.addAll(predicate.terms);
    }

    public boolean isSatisfied(Scan scan) {
        for (Term term : terms) {
            if (!term.isSatisfied(scan)) {
                return false;
            }
        }
        return true;
    }

    public boolean isEmpty() {
        return terms.isEmpty();
    }

    @Override
    public String toString() {
        Iterator<Term> iter = terms.iterator();

        StringBuilder result = new StringBuilder();
        while (iter.hasNext()) {
            result.append(iter.next());
            if (iter.hasNext()) {
                result.append(" AND ");
            }
        }

        return result.toString();
    }
}

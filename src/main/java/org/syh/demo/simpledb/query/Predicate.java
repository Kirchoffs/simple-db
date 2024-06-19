package org.syh.demo.simpledb.query;

import java.util.ArrayList;
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
}

package org.syh.demo.simpledb.parse;

import org.syh.demo.simpledb.plan.Plan;
import org.syh.demo.simpledb.query.Scan;
import org.syh.demo.simpledb.record.Schema;

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

    public void addTerm(Term term) {
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

    public int reductionFactor(Plan plan) {
        int factor = 1;
        for (Term term : terms) {
            factor *= term.reductionFactor(plan);
        }
        return factor;
    }

    public Predicate selectSubPredicates(Schema schema) {
        Predicate result = new Predicate();
        for (Term term : terms) {
            if (term.appliesTo(schema)) {
                result.addTerm(term);
            }
        }
        if (result.isEmpty()) {
            return null;
        }
        return result;
    }

    public Predicate joinSubPredicates(Schema schemaAlpha, Schema schemaBeta) {
        return null;
    }

    public boolean isEmpty() {
        return terms.isEmpty();
    }

    public Constant equatesWithConstant(String fieldName) {
        for (Term term : terms) {
            Constant constant = term.equatesWithConstant(fieldName);
            if (constant != null) {
                return constant;
            }
        }
        return null;
    }

    public String equatesWithField(String fieldName) {
        for (Term term : terms) {
            String field = term.equatesWithField(fieldName);
            if (field != null) {
                return field;
            }
        }
        return null;
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

package org.syh.demo.simpledb.parse;

public class CreateViewData {
    private String viewName;
    private QueryData queryData;

    public CreateViewData(String viewName, QueryData queryData) {
        this.viewName = viewName;
        this.queryData = queryData;
    }

    public String getViewName() {
        return viewName;
    }

    public String getQueryData() {
        return queryData.toString();
    }

    @Override
    public String toString() {
        return "CREATE VIEW " + viewName + " AS " + queryData;
    }
}

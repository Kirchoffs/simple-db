# Notes
### General Idea
- `statement` is composed of `expression`. 
- `expression` is composed of `term`.
- `term` is composed of `factor`. 
- `factor` is composed of `primary`.

simple-db does not follow the above structure actually.

### Example
```
result = (a + b) * (c - d) / e;
```

- Statement: `result = (a + b) * (c - d) / e + f * g;`
- Expression: `(a + b) * (c - d) / e + f * g`
- Term: `(a + b) * (c - d) / e`, `f * g`
- Factor: `(a + b)`, `(c - d)`, `e`, `f`, `g`
- Primary: `a`, `b`, `c`, `d`, `e`, `f`, `g`

`Primary` and `Factor` are almost the same, but sometimes `Primary` focuses more on the basic elements to some extent.  

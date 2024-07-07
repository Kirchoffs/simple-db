# Notes
## Cost of Product Scan
```
B(s) = B(s1) + R(s1) * B(s2)
     = B(s1) + R(s1) / B(S1) * B(s1) * B(s2)
    
let RPB (records per block) = R(s1) / B(s1)
then:
B(s) = B(s1) + RPB(s1) * B(s1) * B(s2)
``` 

In short, the lower the RPB (Records Per Block) of the left relation, the lower the cost of product scan.

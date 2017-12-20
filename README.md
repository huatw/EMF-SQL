## EMF-Query

## About
ESQL -> JAVA CODE -> Query Result

#### ESQL
```sql
SELECT prod, month, count(z.quant)
FROM sales
GROUP BY prod, month; x, y, z
SUCH THAT x.prod = prod and x.month = x.month-1,
          y.prod = prod and y.month = y.month+1,
          z.prod = prod and z.month = month 
                        and z.sale > avg(x.sale) 
                        and z.sale < avg(y.sale)
```

#### QUERY INFORMATION(PHI and more)
* PROJECTIONS: prod, month, count(z.quant)
* GROUPING ATTRIBUTES: prod, month
* GROUPING VARAIBLES:
    1. x.prod = prod and x.month = x.month-1
    2. y.prod = prod and y.month = y.month+1
    3. z.prod = prod and z.month = month and z.sale > avg(x.sale) and z.sale < avg(y.sale)
* HAVING: None
* WHERE: None

#### CODE GENERATION
parts of generated code:

* static parts:
  1. database setup: connect close queryAll
  2. helper: compare varaible

* dynamically generated:
  1. mf-table: aggregate fns, grouping attributes
  2. main fn: init mf table / update aggregates / output


#### LIMITATION:
* suchthat having transformation(naive replacement -> parsing)
* ESQL error checking, handling(parsing needed)


## USAGE
See QTest.java file for examples.

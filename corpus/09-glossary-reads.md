# Reading the glossary and the test results

An agent that answers a question first reads what is known about the
data. The language has two reads for that, `GLOSSARY()` and `TESTS()`,
used in plain SQL. The example reads a table's annotations, every
annotation of one column, and the failing results of one test.

```glossql
SELECT * FROM GLOSSARY(fin.orders);
SELECT * FROM GLOSSARY(fin.orders.amount, all => true);
SELECT subject, state FROM TESTS(fin.trial_balance) WHERE state = 'red';
```

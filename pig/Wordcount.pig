
--SET default_parallel $reducers;

--orders = load '$input/orders' USING PigStorage('|') as (o_orderkey:long, o_custkey:long, o_orderstatus:chararray, o_totalprice:double, o_orderdate:chararray, o_orderpriority:chararray, o_clerk:chararray, o_shippriority:long, o_comment:chararray);

--lineitem = load '$input/lineitem' USING PigStorage('|') as (l_orderkey:long, l_partkey:long, l_suppkey:long, l_linenumber:long, l_quantity:double, l_extendedprice:double, l_discount:double, l_tax:double, l_returnflag:chararray, l_linestatus:chararray, l_shipdate:chararray, l_commitdate:chararray, l_receiptdate:chararray,l_shippingstruct:chararray, l_shipmode:chararray, l_comment:chararray);

--flineitem = FILTER lineitem BY l_shipmode MATCHES 'MAIL|SHIP' AND
--                   l_commitdate < l_receiptdate AND l_shipdate < l_commitdate AND
--					l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01';

--l1 = JOIN flineitem BY l_orderkey, orders by o_orderkey;
--sell1 = FOREACH l1 GENERATE l_shipmode, o_orderpriority;

--grpResult = GROUP sell1 BY l_shipmode;
--sumResult = FOREACH grpResult{
    --urgItems = FILTER sell1 BY o_orderpriority MATCHES '1-URGENT' or o_orderpriority MATCHES '2-HIGH';
  --  GENERATE group, COUNT(urgItems), COUNT(sell1) - COUNT (urgItems);
--};
--sortResult = ORDER sumResult BY group;

--store sumResult into '$output/Q12out' USING PigStorage('|');
--store sortResult into '$output/Q12out' USING PigStorage('|');

--store orders into '$output/Q12out' USING PigStorage('|');

-- ########### script starts ############
--    val read = DList(getArgs(0))
--    val parsed = read.map(WikiArticle.parse(_, "\t"))

register piggybank.jar;

--import org.apache.pig.piggybank.evaluation.string;

articles = LOAD '$input' USING PigStorage('\t') as (pageId : long, name : chararray, updated: chararray, xml: chararray, plaintext: chararray);

 --  parsed
 --     .map(x => "\\n" + x.plaintext)
plaintext = FOREACH articles GENERATE plaintext; --CONCAT('\n', plaintext);

 --     .map(_.replaceAll("""\[\[.*?\]\]""", " "))
plaintext_c1 = FOREACH plaintext GENERATE REPLACE($0, '\\[\\[.*?\\]\\]', ' ');

--      .flatMap(_.replaceAll("""\\[nNt]""", " ").split("[^a-zA-Z0-9']+").toSeq)
plaintext_c2 = FOREACH plaintext_c1 GENERATE REPLACE($0, '\\\\[nNt]', ' ');

--words = FOREACH plaintext_c1 GENERATE FLATTEN(TOKENIZE($0)) as word; 
words = FOREACH plaintext GENERATE FLATTEN(STRSPLIT(plaintext, ' ')) as word; --[^a-zA-Z0-9]+')) as word;

--      .filter(x => x.length > 1)
filtered_words = FILTER words BY org.apache.pig.piggybank.evaluation.string.LENGTH(word) > 1;

--      .filter(x => !stopWordsSet.contains(x))
-- TODO

--      .map(x => if (x.matches("^((left|right)*(thumb)(nail|left|right)*)+[0-9A-Z].*?")) x.replaceAll("((left|right)*thumb(nail|left|right)*)+", "") else x)

-- TODO

--      .map(x => (x, unit(1)))
--      .groupByKey
--      .reduce(_ + _)
word_groups = GROUP filtered_words BY word;
word_count = FOREACH word_groups GENERATE COUNT(filtered_words) AS count, group AS word;

--      .save(getArgs(1))
STORE word_count INTO 'output';

--words = FOREACH input_lines GENERATE FLATTEN(TOKENIZE(line)) AS word;
 
-- filter out any words that are just white spaces
--filtered_words = FILTER words BY word MATCHES '\\w+';
 
-- create a group for each word
-- word_groups = GROUP filtered_words BY word;
 
-- count the entries in each group
-- word_count = FOREACH word_groups GENERATE COUNT(filtered_words) AS count, group AS word;
 
--STORE ordered_word_count INTO '/tmp/number-of-words-on-internet';



--      //      .flatMap(_.replaceAll("""((\\n((\d+px)?(left|right)?thumb(nail)?)*)|(\.(thumb)+))""", " ")
--      //          .split("[^a-zA-Z0-9']+").toSeq)
--      //      .flatMap(_.split("[^a-zA-Z0-9']+").toSeq)
--
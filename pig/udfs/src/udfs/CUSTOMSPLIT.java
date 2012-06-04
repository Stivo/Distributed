package udfs;


import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class CUSTOMSPLIT extends EvalFunc<DataBag>
{
	BagFactory mBagFactory = BagFactory.getInstance();
	TupleFactory mTupleFactory = TupleFactory.getInstance();
	Pattern p = Pattern.compile("[^a-zA-Z0-9\']+");
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try{
        	DataBag out = mBagFactory.newDefaultBag();
            String str = (String)input.get(0);
            if (str != null) 
            for (String word : p.split(str)) {
            	Tuple newTuple = mTupleFactory.newTuple(1);
            	newTuple.set(0, word);
            	out.add(newTuple);
            }
            return out;
        }catch(Exception e){
        	throw new RuntimeException(e);
            //throw WrappedIOException.wrap("Caught exception processing input row ", e);
        }
    }
}
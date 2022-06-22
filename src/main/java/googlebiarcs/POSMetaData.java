package googlebiarcs;

import com.sun.xml.bind.v2.util.QNameMap;

import java.util.HashSet;
import java.util.Set;

public class POSMetaData {
    public final static Set<String> VERB_TYPES =new HashSet<String>(){{
        add("VB");
        add("VBD");
        add("VBG");
        add("VBN");
        add("VBP");
        add("VBZ");
    }};
    public final static Set<String> NOUN_TYPES =new HashSet<String>(){{
        add("NN");
        add("NNS");
        add("NNP");
        add("NNPS");
    }};
}

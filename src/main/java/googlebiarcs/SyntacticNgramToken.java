package googlebiarcs;

public class SyntacticNgramToken {
    public String getWord() {
        return word;
    }

    private String word;
    private String posTag;
    private String depLable;
    private int headIndex;

    public int getSelfIndex() {
        return selfIndex;
    }

    private int selfIndex;
    public SyntacticNgramToken(String biarcString, int selfIndex) throws SyntacticNgramHasNot4Parts {
        String[] splitted = biarcString.split("/");
        if(splitted.length!=4){
            throw new SyntacticNgramHasNot4Parts(biarcString);
        }
        word = splitted[0];
        posTag = splitted[1];
        depLable=splitted[2];
        headIndex=Integer.parseInt(splitted[3]);
        this.selfIndex = selfIndex;
    }

    public String getPosTag() {
        return posTag;
    }

    public int getHeadIndex() {
        return headIndex;
    }
}

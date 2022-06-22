package googlebiarcs;

import org.tartarus.snowball.ext.EnglishStemmer;

import java.util.LinkedList;

public class DependecyPath {
    private LinkedList<SyntacticNgramToken> fromX;
    private LinkedList<SyntacticNgramToken> fromY;

    public DependecyPath(LinkedList<SyntacticNgramToken> fromX, LinkedList<SyntacticNgramToken> fromY) {
        this.fromX = fromX;
        this.fromY = fromY;
    }

    public boolean slotXTypeIsNoun() {
        return POSMetaData.NOUN_TYPES.contains(fromX.getFirst().getPosTag());
    }

    public boolean slotYTypeIsNoun() {
        return POSMetaData.NOUN_TYPES.contains(fromY.getFirst().getPosTag());
    }

    public String getPathStemmedK() {
        String fromXStr = "X";
        String toYStr = "Y";
        for (int i = 1; i < fromX.size()-1; i++) {
            fromXStr+=" "+fromX.get(i).getWord();
        }
        EnglishStemmer stemmer = new EnglishStemmer();
        stemmer.setCurrent(fromX.get(fromX.size()-1).getWord());
        stemmer.stem();
        String head = stemmer.getCurrent();
        for (int i = fromY.size()-2; i > 0; i--) {
            toYStr = fromY.get(i).getWord()+" "+toYStr;
        }
        return fromXStr + " "+head+" "+toYStr;
    }

    public String getX() {
        return fromX.getFirst().getWord();
    }

    public String getY() {
        return fromY.getFirst().getWord();
    }
}

package googlebiarcs;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class ParsedBiarcs {
    private SyntacticNgramToken[] syntacticNgramTokens;
    private SyntacticNgramToken head;
    private String rawLine;
    private int count;

    public ParsedBiarcs(String rawLine) throws SyntacticNgramHasNot4Parts {
        this.rawLine = rawLine;
        String[] splitted = rawLine.split("\t");
        String[] biarcsStrings = splitted[1].split(" ");
        syntacticNgramTokens = new SyntacticNgramToken[biarcsStrings.length];
        for (int i = 0; i < syntacticNgramTokens.length; i++) {
            syntacticNgramTokens[i] = new SyntacticNgramToken(biarcsStrings[i], i+1);
            if(syntacticNgramTokens[i].getHeadIndex() == 0){
                head = syntacticNgramTokens[i];
            }
        }
        assert head!=null;
        count = Integer.parseInt(splitted[2]);
    }

    public boolean headIsVerb() {
        return POSMetaData.VERB_TYPES.contains(head.getPosTag());
    }

    public List<DependecyPath> getDependencyPaths() {
        boolean[] status = new boolean[syntacticNgramTokens.length];
        List<LinkedList<SyntacticNgramToken>> pathsToSlot = new ArrayList<>();
        LinkedList<SyntacticNgramToken> onlyHead = new LinkedList<>();
        onlyHead.add(head);
        status[head.getSelfIndex()-1] = true;
        Queue<LinkedList<SyntacticNgramToken>> queue = new LinkedList<>();
        queue.add(onlyHead);
        while (!queue.isEmpty()){
            int modifiers = 0;
            LinkedList<SyntacticNgramToken> currPath = queue.poll();
            SyntacticNgramToken curr = currPath.getFirst();
            for (int i = 0; i < syntacticNgramTokens.length; i++) {
                if(syntacticNgramTokens[i].getHeadIndex() == curr.getSelfIndex()){
                    if(status[i]) throw new RuntimeException("circle "+rawLine);
                    LinkedList<SyntacticNgramToken> newLL = new LinkedList<>(currPath);
                    newLL.addFirst(syntacticNgramTokens[i]);
                    queue.add(newLL);
                    status[i]=true;
                    modifiers++;
                }
            }
            if (modifiers == 0){
                pathsToSlot.add(currPath);
            }
        }
        List<DependecyPath> ans = new ArrayList<>();
        for (LinkedList<SyntacticNgramToken> pathToSolt1 :
                pathsToSlot) {
            for (LinkedList<SyntacticNgramToken> pathToSlot2 :
                    pathsToSlot) {
                if (pathToSolt1.getFirst().getSelfIndex() < pathToSlot2.getFirst().getSelfIndex()) {
                    ans.add(new DependecyPath(pathToSolt1, pathToSlot2));
                }
            }
        }
        return ans;
    }

    public int getCount() {
        return count;
    }
}

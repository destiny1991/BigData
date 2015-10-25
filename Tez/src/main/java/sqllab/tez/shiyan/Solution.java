package sqllab.tez.shiyan;

import java.util.*;

public class Solution {
    String[] key = {"abc", "def", "ghi", "jkl",
                    "mno", "pqrs", "tuv", "wxyz"};
    
    List<String> combination(String str, List<String> res) {
        List<String> ans = new ArrayList<>();
        
        for(char c: str.toCharArray()) {
        	for(String s: res) ans.add(s+c); 
        }
        return ans;
    }
    
    public List<String> letterCombinations(String digits) {
        List<String> res = new ArrayList<>();
        res.add("");
        for(char c: digits.toCharArray()) {
            res = combination(key[c-'0'-2], res);
        }
        
        return res;
    }
}

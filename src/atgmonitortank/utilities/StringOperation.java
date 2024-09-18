/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package atgmonitortank.utilities;

/**
 *
 * @author zakhiyah arsal
 */
public class StringOperation {
    public static String InsertString (String rawValue) {
        StringBuffer fin_rawValue = new StringBuffer(rawValue);
        int length_rawValue = rawValue.length();
                switch (length_rawValue) {
                    case 3:
                        fin_rawValue.insert(0,"0");
                        break;
                    case 2:
                        fin_rawValue.insert(0,"00");
                        break;
                    case 1:
                        fin_rawValue.insert(0,"000");
                        break;
                }
        
        return fin_rawValue.toString();
    }
    
}

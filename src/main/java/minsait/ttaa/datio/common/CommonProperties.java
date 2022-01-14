package minsait.ttaa.datio.common;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class CommonProperties {

    private Map<String, String> propMap;

    /**
     * Getting input and output paths from config.properties file
     * @throws IOException
     */
    public CommonProperties() throws IOException {
        InputStream is = null;
        propMap = new HashMap<String, String>();
        try{
            Properties prop = new Properties();
            String propFileName = "config.properties";

            is = getClass().getClassLoader().getResourceAsStream(propFileName);

            if(is!=null){
                prop.load(is);
            }else{
                throw new FileNotFoundException("Properties file not found");
            }

            propMap.put("input_data", prop.getProperty("input_data"));
            propMap.put("output_folder", prop.getProperty("output_folder"));
        }catch (Exception e){
            System.out.println("Exception " + e);
        } finally {
            is.close();
        }
    }

    public Map<String, String> getProperties() {
        return propMap;
    }
}

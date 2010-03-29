package benchmarks.interfaces;

import java.util.Map;

public interface ProbabilityDistribution {

    public void init(int numberElements, Map<String,Object> options);

    public void setInfo(Map<String,String> info); 

    public int getNextElement();

    public ProbabilityDistribution getNewInstance();

}

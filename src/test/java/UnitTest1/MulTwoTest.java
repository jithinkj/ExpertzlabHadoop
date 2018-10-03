package UnitTest1;

import org.junit.Test;

import static org.junit.Assert.*;

public class MulTwoTest {
@Test
    public void testMulTwo(){
    int a=5;
    int b=6;
    MulTwo ob=new MulTwo();
    ob.mult(a,b);
    System.out.println("result= "+ob.result);
    assertEquals(30,ob.result);
}
}
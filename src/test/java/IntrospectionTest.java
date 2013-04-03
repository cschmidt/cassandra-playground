import java.lang.reflect.Field;
import java.sql.Types;

import org.junit.Test;




public class IntrospectionTest {

    
    @Test
    public void testBasic() throws IllegalArgumentException, IllegalAccessException {
        Field[] fields = Types.class.getDeclaredFields();
        for( Field f : Types.class.getDeclaredFields() ) {
            f.setAccessible(true);
            System.out.println(f);
            System.out.println(f.get(null));
        }
    }
}

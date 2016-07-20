import io.mindmaps.api.GraphFactoryController;
import io.mindmaps.api.RestGETController;
import io.mindmaps.api.VisualiserController;

public class MindmapsEngineServer {


    public static void main(String[] args) {

        new RestGETController();
        new VisualiserController();
        new GraphFactoryController();

    }
}

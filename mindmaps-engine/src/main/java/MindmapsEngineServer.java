import api.GraphFactoryController;
import api.RestGETController;
import api.VisualiserController;

public class MindmapsEngineServer {


    public static void main(String[] args) {

        new RestGETController();
        new VisualiserController();
        new GraphFactoryController();

    }
}

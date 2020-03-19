package testing;

import grakn.client.GraknClient;
public class Main {

    public static void main(String args[]) {
        GraknClient client = new GraknClient("localhost:48555");
        System.out.println("hi");
    }
}

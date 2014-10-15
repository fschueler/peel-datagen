package eu.stratosphere.peel.datagen.spark;

import eu.stratosphere.peel.datagen.AppRunner;

public class App {

    public static void main(String[] args) {
        AppRunner runner = new AppRunner("eu.stratosphere.peel.datagen.spark", "peel-datagen-spark", "Spark");
        runner.run(args);
    }
}

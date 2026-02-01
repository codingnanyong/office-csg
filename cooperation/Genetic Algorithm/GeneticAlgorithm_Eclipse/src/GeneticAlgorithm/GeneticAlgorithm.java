package GeneticAlgorithm;

import java.util.Random;
import java.util.Scanner;

public class GeneticAlgorithm {

    Population population = new Population();
    Individual fittest;
    Individual secondFittest;
    Individual saveFittest;
    int generationCount = 0;
    int taktTime = 0;
    int reformationCount = 0;

    GeneticAlgorithm() {

        setTaktTime();
        setBeginning();
        GeneticAlgorithmAction();

    }

    public void setTaktTime() {

        try (Scanner sc = new Scanner(System.in)) {
            System.out.println("taktTime?");
            taktTime = sc.nextInt();
        }
    }

    public void setBeginning() {

        population.initializePopulation(10, taktTime);
        population.calculateFitness(taktTime);
        fittest = Individual.newInstance(population.getFittest());
        saveFittest = Individual.newInstance(fittest);

    }

    public void GeneticAlgorithmAction() {

        while (generationCount < 10000) {
            ++generationCount;
            if (reformationCount >= 500) {
                break;
            }

            selection();
            crossover();

            if (fittest.wk.getStitchingCount() < fittest.wk.getPrefitCount()) {
                if (generationCount % 5 == 0) {
                    mutation();
                }
            }

            addFittestOffspring();
            population.calculateFitness(taktTime);

            if (fittest.fitness > saveFittest.fitness) {
                saveFittest = Individual.newInstance(fittest);
                reformationCount = 0;
            } else {
                reformationCount++;
            }
        }
    }

    public void printResult() {

        saveFittest.changeGenes();
        saveFittest.printMatrix(generationCount);
    }

    public void selection() {

    	fittest = Individual.newInstance(population.getFittest());
    	secondFittest = Individual.newInstance(population.getSecondFittest());
        //fittest = population.getFittest();
        //secondFittest = population.getSecondFittest();
    }

    public void crossover() {

        Random rn = new Random();
        int row = population.individuals[0].genes.length;
        int cloumn = population.individuals[0].genes[0].length;
        int[] crossOverPoint = new int[2];

        for (int i = 0; i < crossOverPoint.length; i++) {
            for (int k = 0; k < i; k++) {
                if (crossOverPoint[i] == crossOverPoint[k]) {
                    i--;
                }
            }
            crossOverPoint[i] = rn.nextInt(row);
        }

        int[][] temp = new int[1][cloumn];

        for (int i = 0; i < row; i++) {
            if (i == crossOverPoint[0]) {
                for (int j = 0; j < cloumn; j++) {
                    temp[0][j] = fittest.genes[i][j];
                    fittest.genes[i][j] = secondFittest.genes[crossOverPoint[1]][j];
                    secondFittest.genes[crossOverPoint[1]][j] = temp[0][j];
                }
            }
        }
    }

    public void mutation() {

        Random rn = new Random();
        int row = population.individuals[0].genes.length;
        int cloumn = population.individuals[0].genes[0].length;
        int[] index = new int[row];

        for (int i = 0; i < row; i++) {
            for (int j = 0; j < cloumn; j++) {
                if (fittest.genes[i][j] == 1) {
                    index[i] = j;
                }
            }
        }

        int IdentityMatrix_point = 0;
        boolean Validation = false;

        while (!Validation) {
            IdentityMatrix_point = rn.nextInt(cloumn);
            for (int i = 0; i < index.length; i++) {
                if (IdentityMatrix_point == index[i] || IdentityMatrix_point == 0) {
                    Validation = false;
                    break;
                } else {
                    Validation = true;
                }
            }
        }

        int mutationIndex = rn.nextInt(cloumn);

        for (int i = 0; i < row; i++) {
            if (i == mutationIndex) {
                for (int j = 0; j < cloumn; j++) {
                    fittest.genes[i][j] = population.getFittest().Identity_Matrix()[IdentityMatrix_point][j];
                }
                index[i] = mutationIndex;
            }
        }
    }

    public Individual getFittestOffspring() {

        if (fittest.fitness > secondFittest.fitness) {return fittest;}
        return secondFittest;
    }

    public void addFittestOffspring() {

        fittest.calcFitness(taktTime);
        secondFittest.calcFitness(taktTime);

        int leastFittestIndex = population.getLeastFittestIndex();
        population.individuals[leastFittestIndex] = getFittestOffspring();
    }
}

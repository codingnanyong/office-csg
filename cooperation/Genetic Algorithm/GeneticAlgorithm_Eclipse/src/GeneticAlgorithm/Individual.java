package GeneticAlgorithm;

import java.util.Random;

public class Individual {

    WorkCondition wk = new WorkCondition();

    double fitness = 0;
    int[][] genes;
    double[][] Result;
    int[] index;
    int[] temp;

    public static Individual newInstance(Individual individual) {

        Individual newIndividual = new Individual();
        newIndividual.genes = individual.genes;
        newIndividual.Result = individual.Result;
        newIndividual.index = individual.index;
        newIndividual.fitness = individual.fitness;
        newIndividual.temp = individual.temp;
        return newIndividual;
    }

    public Individual() {

       genes = new int[wk.getStitchingCount()][wk.getPrefitCount()];
       Result = new double[wk.getStitchingCount()][1];
       index = new int[wk.getStitchingCount()];
       temp = new int [wk.getStitchingCount()];
       genes = Candidate();
    }

    // Create Identity_Matrix
    public int[][] Identity_Matrix() {

        int[][] Identity_Matrix;

        if (wk.getStitchingCount() > wk.getPrefitCount()) {
            Identity_Matrix = new int[wk.getStitchingCount() + 1][wk.getStitchingCount() + 1];
        } else {
            Identity_Matrix = new int[wk.getPrefitCount() + 1][wk.getPrefitCount() + 1];
        }
        for (int i = 0; i < Identity_Matrix.length; i++) {
            for (int j = 0; j < Identity_Matrix[i].length; j++) {
                if (i == 0) {
                    Identity_Matrix[i][j] = 0;
                } else {
                    if (i == j) {
                        Identity_Matrix[i][j] = 1;
                    } else {
                        Identity_Matrix[i][j] = 0;
                    }
                }
            }
        }
        return Identity_Matrix;
    }

    // Set genes randomly for each individual
    public int[][] Candidate() {

        int[][] Identity_Matrix = Identity_Matrix();
        int[] random = new int[wk.getStitchingCount()];
        Random rn = new Random();

        for (int i = 0; i < genes.length; i++) {
            random[i] = rn.nextInt(wk.getPrefitCount());
            for (int k = 0; k < i; k++) {
                if (random[i] == random[k]) {
                    i--;
                }
            }
            for (int j = 0; j < genes[i].length; j++) {
                genes[i][j] = Identity_Matrix[random[i] + 1][j];
            }
        }

        for (int i = 0; i < genes.length; i++) {
            for (int j = 0; j < genes[i].length; j++) {
                if (genes[i][j] == 1) {
                    index[i] = j;
                }
            }

        }
        Result = MatrixCalculation();
        return genes;
    }

    public double[][] MatrixCalculation() {

        Result = multiplication(genes, wk.getPrefit());
        Result = addition(wk.getStitching(), Result);
        Result = division(Result, wk.getWorkers());
        return Result;
    }

    // matrix multiplication
    public double[][] multiplication(int[][] arr1, int[][] arr2) {

        double[][] answer = new double[arr1.length][arr2[0].length];

        for (int i = 0; i < arr1.length; i++) {
            for (int j = 0; j < arr1[0].length; j++) {
                for (int k = 0; k < arr2[0].length; k++) {
                    answer[i][k] += arr1[i][j] * arr2[j][k];
                }
            }
        }
        return answer;
    }

    // matrix addtion
    public double[][] addition(double[][] arr1, double[][] arr2) {

        double[][] answer = new double[arr1.length][arr1[0].length];

        for (int i = 0; i < arr1.length; i++) {
            for (int j = 0; j < arr1[i].length; j++) {
                answer[i][j] = arr1[i][j] + arr2[i][j];
            }
        }
        return answer;
    }

    // matrix cloumn division
    public double[][] division(double[][] arr1, int[][] arr2) {

        double[][] answer = new double[arr1.length][arr1[0].length];

        for (int i = 0; i < arr1.length; i++) {

            for (int j = 0; j < arr1[i].length; j++) {
                answer[i][j] = Double.parseDouble((String.format("%.2f", Math.abs(arr1[i][j] / arr2[i][j]))));
            }
        }
        return answer;
    }

    // matrix row sum
    public double sum(double[][] arr) {

        double sum = 0;

        for (int i = 0; i < arr.length; i++) {

            for (int j = 0; j < arr[i].length; j++) {
                sum += arr[i][j];
            }
        }
        sum = Double.parseDouble(String.format("%.2f", sum));
        return sum;
    }
    
    public void printMatrix(int count) {

        System.out.println("\nSolution found in generation " + count +" Fitness: " + fitness);

        for (int i = 0; i < genes.length; i++) {
            for (int j = 0; j < genes[i].length; j++) {
                System.out.print(genes[i][j] + " ");
            }
            System.out.println();
        }
    }

    public void printObjectMatrix() {

        for (int i = 0; i < Result.length; i++) {
            for (int j = 0; j < Result[i].length; j++) {
                System.out.print(Result[i][j] + " ");
            }
            System.out.println();
        }
    }

    // Calculate fitness(적합도 계산)
    public double calcFitness(int taktTime) {

        for (int i = 0; i < wk.getStitchingCount(); i++) {
            if (taktTime > Result[i][0]) {
                if (i <= wk.getPUS()[index[i]][0]) {
                    temp[i] = 0;
                } else {
                    Result[i][0] -= (wk.getPrefit()[index[i]][0] / wk.getWorkers()[i][0]);
                    temp[i] = 1;
                    break;
                }
            } else {
                Result[i][0] -= (wk.getPrefit()[index[i]][0] / wk.getWorkers()[i][0]);
                temp[i] = 1;
                break;
            }
        }
        
        fitness = sum(Result);
        return fitness;
    }

    public void changeGenes() {

        for (int i = 0; i < wk.getStitchingCount(); i++) {
            if (temp[i] == 1) {
                for (int j = 0; j < wk.getPrefitCount(); j++) {
                    genes[i][j] = 0;
                }
            }
        }
    }
}
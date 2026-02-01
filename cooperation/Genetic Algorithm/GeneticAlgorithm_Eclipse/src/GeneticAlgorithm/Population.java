package GeneticAlgorithm;

class Population {

    int popSize = 10;
    Individual[] individuals = new Individual[10];
    double fittest = 0;

    public void initializePopulation(int size,int taktTime){

        for (int i = 0; i < individuals.length; i++) {
            individuals[i] = new Individual();
            //individuals[i].setTaktTime(taktTime);
        }
    }

    public Individual getFittest() {

        double maxFit = Double.MIN_VALUE;
        int maxFitIndex = 0;

        for (int i = 0; i < individuals.length; i++) {
            if (maxFit <= individuals[i].fitness) {
                maxFit = individuals[i].fitness;
                maxFitIndex = i;
            }
        }
        fittest = individuals[maxFitIndex].fitness;
        return individuals[maxFitIndex];
    }

    public Individual getSecondFittest() {

        int maxFit1 = 0;
        int maxFit2 = 0;

        for (int i = 0; i < individuals.length; i++) {
            if (individuals[i].fitness > individuals[maxFit1].fitness) {
                maxFit2 = maxFit1;
                maxFit1 = i;
            } else if (individuals[i].fitness > individuals[maxFit2].fitness) {
                maxFit2 = i;
            }
        }
        return individuals[maxFit2];
    }

    public int getLeastFittestIndex() {

        double minFitVal = Double.MAX_VALUE;
        int maxFitIndex = 0;
        
        for (int i = 0; i < individuals.length; i++) {
            if (minFitVal >= individuals[i].fitness) {
                minFitVal = individuals[i].fitness;
                maxFitIndex = i;
            }
        }
        return maxFitIndex;
    }

    public void calculateFitness(int taktTime) {

        for (int i = 0; i < individuals.length; i++) {
            individuals[i].calcFitness(taktTime);
        }
        getFittest();
    }

}
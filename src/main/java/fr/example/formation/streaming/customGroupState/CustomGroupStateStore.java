package fr.example.formation.streaming.customGroupState;

public class CustomGroupStateStore {

    int nbProduction;

    public CustomGroupStateStore() {
        this.nbProduction = 0;
    }

    public int getNbProduction() {
        return nbProduction;
    }

    public void setNbProduction(int nbProduction) {
        this.nbProduction = nbProduction;
    }
}

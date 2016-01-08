package itmo.escience.dstorage.agent.utils;

/**
 *
 * @author anton
 */
public enum StorageLevel {
    MEM(2),
    HDD(0),
    SSD(1),
    NOTSET(-2);
    private final int num;
    StorageLevel(int n){this.num=n;}
    public final int getNum(){return this.num;}
    public static StorageLevel getLevelById(int n){
        switch(n){
            case 0: return StorageLevel.HDD;
            case 1: return StorageLevel.SSD;
            case 2: return StorageLevel.MEM;
            default : return StorageLevel.NOTSET;
        }
    }
} 

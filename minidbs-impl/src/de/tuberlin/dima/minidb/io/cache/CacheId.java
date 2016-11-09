package de.tuberlin.dima.minidb.io.cache;

/**
 * Created by arbuzinside on 5.11.2015.
 */
public class CacheId {


    private int pageNumber;
    private int resourceId;

    public CacheId(int number, int id){
        this.setPageNumber(number);
        this.setResourceId(id);

    }

    public int getResourceId() {
        return resourceId;
    }

    public void setResourceId(int resourceId) {
        this.resourceId = resourceId;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public void setPageNumber(int pageNumber) {
        this.pageNumber = pageNumber;
    }



    /*
     * (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + this.resourceId;
        result = prime * result + this.pageNumber;
        return result;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        CacheId other = (CacheId) obj;
        if (this.resourceId != other.resourceId)
            return false;
        if (this.pageNumber != other.pageNumber)
            return false;
        return true;
    }



}

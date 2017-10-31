package ar.edu.itba;

/**
 * Created by root on 10/31/17.
 */
public enum Region {
    BUENOS_AIRES, CENTRO, NUEVO_CUYO, NORTE_GRANDE_ARGENTINO, PATAGONICA;

    public static Region fromString(String province){
        if(province.equals("Buenos Aires")){
            return BUENOS_AIRES;
        }else if (province.equals("Entre Ríos") || province.equals("Santa Fe") || province.equals("Córdoba")){
            return CENTRO;
        }else if(province.equals("La Rioja") || province.equals("San Juan") || province.equals("Mendoza") || province.equals("San Luis")){
            return NUEVO_CUYO;
        }else if(province.equals("Catamarca") || province.equals("Santiago del Estero") || province.equals("Tucumán") || province.equals("Salta") || province.equals("Jujuy") || province.equals("Chaco") || province.equals("Formosa") || province.equals("Corrientes") || province.equals("Misiones")){
            return NORTE_GRANDE_ARGENTINO;
        }else if(province.equals("La Pampa") || province.equals("Neuquén") || province.equals("Río Negro") || province.equals("Chubut") || province.equals("Santa Cruz") || province.equals("Tierra del Fuego")){
            return PATAGONICA;
        }else{
            throw new IllegalArgumentException("Unknown String value " + province);
        }
    }
}

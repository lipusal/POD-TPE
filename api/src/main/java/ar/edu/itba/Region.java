package ar.edu.itba;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 10/31/17.
 */
public enum Region {
    BUENOS_AIRES, CENTRO, NUEVO_CUYO, NORTE_GRANDE_ARGENTINO, PATAGONICA;

    private static final Map<String,Region> regionsMap = new HashMap<>();

    static {
        regionsMap.put("Buenos Aires", Region.BUENOS_AIRES);
        regionsMap.put("Ciudad Autónoma de Buenos Aires", Region.BUENOS_AIRES);
        regionsMap.put("Entre Ríos", Region.CENTRO);
        regionsMap.put("Santa Fe", Region.CENTRO);
        regionsMap.put("Córdoba", Region.CENTRO);
        regionsMap.put("La Rioja", Region.NUEVO_CUYO);
        regionsMap.put("San Juan", Region.NUEVO_CUYO);
        regionsMap.put("Mendoza", Region.NUEVO_CUYO);
        regionsMap.put("San Luis", Region.NUEVO_CUYO);
        regionsMap.put("Catamarca", Region.NORTE_GRANDE_ARGENTINO);
        regionsMap.put("Santiago del Estero", Region.NORTE_GRANDE_ARGENTINO);
        regionsMap.put("Tucumán", Region.NORTE_GRANDE_ARGENTINO);
        regionsMap.put("Salta", Region.NORTE_GRANDE_ARGENTINO);
        regionsMap.put("Jujuy", Region.NORTE_GRANDE_ARGENTINO);
        regionsMap.put("Chaco", Region.NORTE_GRANDE_ARGENTINO);
        regionsMap.put("Formosa", Region.NORTE_GRANDE_ARGENTINO);
        regionsMap.put("Corrientes", Region.NORTE_GRANDE_ARGENTINO);
        regionsMap.put("Misiones", Region.NORTE_GRANDE_ARGENTINO);
        regionsMap.put("La Pampa",Region.PATAGONICA);
        regionsMap.put("Neuquén",Region.PATAGONICA);
        regionsMap.put("Río negro",Region.PATAGONICA);
        regionsMap.put("Chubut",Region.PATAGONICA);
        regionsMap.put("Santa Cruz",Region.PATAGONICA);
        regionsMap.put("Tierra del Fuego",Region.PATAGONICA);

    }
    public static Region fromString(String province){

        if(!regionsMap.containsKey(province)){
            throw new IllegalArgumentException("Unknown String value " + province);
        }

        return regionsMap.get(province);

    }
}

'use client';

import AccordionCustomOpen from "@/components/custom-ui/accordion-custom-open";
import AccordionCustomOpenIframe from "@/components/custom-ui/accordion-custom-open-iframe";
import SelectSearchCustom, {Option} from "@/components/custom-ui/select-search-custom";

export default function ParamStudies() {

    const evolTemporalValues = [
        {
            name: "evol_2024",
            title: "Evolution in 2024",
        },
        {
            name: "evol_yearly_group",
            title: "Evolution by year from the beginning of the records",
        }


    ]
    
    const evolLocationValues: Option[] = [
        {
            value: "a_coruna",
            label: "A Coruña"
        },
        {
            value: "albacete",
            label: "Albacete"
        },
        {
            value: "alicante",
            label: "Alicante"
        },
        {
            value: "almeria",
            label: "Almería"
        },
        {
            value: "araba",
            label: "Araba / Álava"
        },
        {
            value: "asturias",
            label: "Asturias"
        },
        {
            value: "avila",
            label: "Ávila"
        },
        {
            value: "badajoz",
            label: "Badajoz"
        },
        {
            value: "barcelona",
            label: "Barcelona"
        },
        {
            value: "bizkaia",
            label: "Bizkaia"
        },
        {
            value: "burgos",
            label: "Burgos"
        },
        {
            value: "caceres",
            label: "Cáceres"
        },
        {
            value: "cadiz",
            label: "Cádiz"
        },
        {
            value: "cantabria",
            label: "Cantabria"
        },
        {
            value: "castellon",
            label: "Castellón"
        },
        {
            value: "ceuta",
            label: "Ceuta"
        },
        {
            value: "cordoba",
            label: "Córdoba"
        },
        {
            value: "cuenca",
            label: "Cuenca"
        },
        {
            value: "ciudad_real",
            label: "Ciudad Real"
        },
        {
            value: "gipuzkoa",
            label: "Gipuzkoa"
        },
        {
            value: "girona",
            label: "Girona"
        },
        {
            value: "granada",
            label: "Granada"
        },
        {
            value: "guadalajara",
            label: "Guadalajara"
        },
        {
            value: "huelva",
            label: "Huelva"
        },
        {
            value: "huesca",
            label: "Huesca"
        },
        {
            value: "illes_balears",
            label: "Illes Balears"
        },
        {
            value: "jaen",
            label: "Jaén"
        },
        {
            value: "las_palmas",
            label: "Las Palmas"
        },
        {
            value: "la_rioja",
            label: "La Rioja"
        },
        {
            value: "leon",
            label: "León"
        },
        {
            value: "lleida",
            label: "Lleida"
        },
        {
            value: "lugo",
            label: "Lugo"
        },
        {
            value: "madrid",
            label: "Madrid"
        },
        {
            value: "malaga",
            label: "Málaga"
        },
        {
            value: "melilla",
            label: "Melilla"
        },
        {
            value: "murcia",
            label: "Murcia"
        },
        {
            value: "navarra",
            label: "Navarra"
        },
        {
            value: "ourense",
            label: "Ourense"
        },
        {
            value: "palencia",
            label: "Palencia"
        },
        {
            value: "pontevedra",
            label: "Pontevedra"
        },
        {
            value: "salamanca",
            label: "Salamanca"
        },
        {
            value: "santa_cruz_de_tenerife",
            label: "Santa Cruz de Tenerife"
        },
        {
            value: "segovia",
            label: "Segovia"
        },
        {
            value: "sevilla",
            label: "Sevilla"
        },
        {
            value: "soria",
            label: "Soria"
        },
        {
            value: "tarragona",
            label: "Tarragona"
        },
        {
            value: "teruel",
            label: "Teruel"
        },
        {
            value: "toledo",
            label: "Toledo"
        },
        {
            value: "valencia",
            label: "Valencia"
        },
        {
            value: "valladolid",
            label: "Valladolid"
        },
        {
            value: "zamora",
            label: "Zamora"
        },
        {
            value: "zaragoza",
            label: "Zaragoza"
        },
    ]

    const substudiesValues = {
        top10: {
            name: "top_10",
            title: "Top 10",
            values: [
                {
                    name: "highest",
                    title: "Highest",
                    values: [
                        {
                            name: "2024",
                            title: "2024",
                        },
                        {
                            name: "decade",
                            title: "Last Decade",
                        },
                        {
                            name: "global",
                            title: "From the beginning of the records",
                        }
                    ]
                },
                {
                    name: "lowest",
                    title: "Lowest",
                    values: [
                        {
                            name: "2024",
                            title: "2024",
                        },
                        {
                            name: "decade",
                            title: "Last Decade",
                        },
                        {
                            name: "global",
                            title: "From the beginning of the records",
                        }
                    ]
                }
            ]
        },
        top5inc: {
            name: "top_5_inc",
            title: "Top 5 increments",
            values: [
                {
                    name: "highest",
                    title: "Highest"
                },
                {
                    name: "lowest",
                    title: "Lowest",
                }
            ]
        },
        evol: {
            name: "evol",
            title: "Evolution",
            locationValues: evolLocationValues,
            temporalValues: evolTemporalValues
        },
        heatMap2024: {
            name: "heat_map_2024",
            title: "Spanish territory heat map in 2024",
            values: [
                {
                    name: "continental",
                    title: "Continental territory",
                },
                {
                    name: "canary_islands",
                    title: "Canary islands territory",
                },
            ]
        }
    }

    const meteorologicalParamValues = [
        {
            name: "temp",
            title: "Temperature",
            substudies: substudiesValues
        },
        {
            name: "prec",
            title: "Precipitation",
            substudies: substudiesValues
        },
        {
            name: "wind_vel",
            title: "Wind velocity",
            substudies: substudiesValues
        },
        {
            name: "press",
            title: "Atmospheric pressure",
            substudies: substudiesValues
        },
        {
            name: "sun_rad",
            title: "Sun radiation",
            substudies: substudiesValues
        },
        {
            name: "rel_hum",
            title: "Relative humidity",
            substudies: substudiesValues
        },
    ]

    return (
        <section className="p-4 bg-white rounded shadow mb-4">
            <h2 className="text-xl font-bold mb-2">Meteorological parameter studies</h2>
            <p>Studies based on a specific climatic parameter in the Spanish territory.</p>

            {
                meteorologicalParamValues.map((meteoParam, meteoParamIdx) => (
                    <AccordionCustomOpen key={meteoParamIdx} title={meteoParam.title} id={`single_param_studies-${meteoParam.name}`}>

                        {/*TOP 10*/}
                        <AccordionCustomOpen title={meteoParam.substudies.top10.title} id={`single_param_studies-${meteoParam.substudies.top10.name}`}>
                        {
                            meteoParam.substudies.top10.values.map((order, orderIdx) => (
                                <AccordionCustomOpen key={orderIdx} title={order.title} id={`single_param_studies-${order.name}`}>
                                {
                                    order.values.map((temporal, temporalIdx) => (
                                        <AccordionCustomOpenIframe
                                            key={temporalIdx}
                                            title={temporal.title}
                                            src={`https://c0nf1cker.net/single_param_studies/${meteoParam.name}/${meteoParam.substudies.top10.name}/${order.name}/${temporal.name}/plot.html`}
                                            id={`single_param_studies-${meteoParam.name}-${meteoParam.substudies.top10.name}-${order.name}-${temporal.name}`}
                                        ></AccordionCustomOpenIframe>
                                    ))
                                }
                                </AccordionCustomOpen>
                            ))
                        }
                        </AccordionCustomOpen>

                        {/*TOP 5 INC*/}
                        <AccordionCustomOpen title={meteoParam.substudies.top5inc.title} id={`single_param_studies-${meteoParam.substudies.top5inc.name}`}>
                        {
                            meteoParam.substudies.top5inc.values.map((order, orderIdx) => (
                                <AccordionCustomOpenIframe
                                    key={orderIdx}
                                    title={order.title}
                                    src={`https://c0nf1cker.net/single_param_studies/${meteoParam.name}/${meteoParam.substudies.top5inc.name}/${order.name}/plot.html`}
                                    id={`single_param_studies-${meteoParam.name}-${meteoParam.substudies.top5inc.name}-${order.name}`}
                                ></AccordionCustomOpenIframe>
                            ))
                        }
                        </AccordionCustomOpen>

                        {/*EVOL*/}
                        <AccordionCustomOpen title={meteoParam.substudies.evol.title} id={`single_param_studies-${meteoParam.substudies.evol.name}`}>
                        {
                            <SelectSearchCustom options={meteoParam.substudies.evol.locationValues} childrenFn={({value}: Option) => (
                                meteoParam.substudies.evol.temporalValues.map((temporal, temporalIdx) => (
                                    <AccordionCustomOpenIframe
                                        key={temporalIdx}
                                        title={temporal.title}
                                        src={`https://c0nf1cker.net/single_param_studies/${meteoParam.name}/${meteoParam.substudies.evol.name}/${value}/${temporal.name}/plot.html`}
                                        id={`single_param_studies-${meteoParam.name}-${meteoParam.substudies.evol.name}-${value}-${temporal.name}`}
                                    ></AccordionCustomOpenIframe>
                                ))
                            )}></SelectSearchCustom>
                        }
                        </AccordionCustomOpen>

                        {/*HEAT MAP 2024*/}
                        <AccordionCustomOpen title={meteoParam.substudies.heatMap2024.title} id={`single_param_studies-${meteoParam.substudies.heatMap2024.name}`}>
                        {
                            meteoParam.substudies.heatMap2024.values.map((location, locationIdx) => (
                                <AccordionCustomOpenIframe
                                    key={locationIdx}
                                    title={location.title}
                                    src={`https://c0nf1cker.net/single_param_studies/${meteoParam.name}/${meteoParam.substudies.heatMap2024.name}/${location.name}/plot.html`}
                                    id={`single_param_studies-${meteoParam.name}-${meteoParam.substudies.top5inc.name}-${location.name}`}
                                ></AccordionCustomOpenIframe>
                            ))
                        }
                        </AccordionCustomOpen>

                    </AccordionCustomOpen>
                ))
            }
        </section>
    )
}
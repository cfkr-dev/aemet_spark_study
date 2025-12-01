'use client';

import AccordionCustomOpen from "@/components/custom-ui/accordion-custom-open";
import AccordionCustomOpenIframe from "@/components/custom-ui/accordion-custom-open-iframe";
import SelectSearchCustom, {Option} from "@/components/custom-ui/select-search-custom";

export default function InterestingStudies() {

    const precAndPressEvolTemporalValues = [
        {
            name: "evol_2024",
            title: "Evolution in 2024",
        },
        {
            name: "evol_yearly_group",
            title: "Evolution by year from the beginning of the records",
        }


    ]
    
    const precAndPressEvolLocationValues: Option[] = [
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

    const top10StatesValues = [
        {
            name: "better_sun_power",
            title: "Best states for sun power"
        },
        {
            name: "better_wind_power",
            title: "Best states for wind power"
        },
        {
            name: "agriculture",
            title: "Best states to practice agriculture"
        },
        {
            name: "torrential_rains",
            title: "States with the highest incidence of torrential rains"
        },
        {
            name: "storms",
            title: "States with the highest incidence of storms"
        },
        {
            name: "heat_waves",
            title: "States with the highest incidence of heat waves"
        },
        {
            name: "frosts",
            title: "States with the highest incidence of frosts"
        },
        {
            name: "fires",
            title: "States with the highest incidence of fires"
        },
        {
            name: "droughts",
            title: "States with the highest incidence of droughts"
        },
    ]

    const interestingStudiesValues = {
        precAndPressEvol: {
            name: "prec_and_press_evol",
            title: "Precipitation and atmospheric pressure evolution",
            locationValues: precAndPressEvolLocationValues,
            temporalValues: precAndPressEvolTemporalValues
        },
        top10States: {
            name: "top_10_states",
            title: "Top 10 better states for special conditions",
            values: top10StatesValues
        }
    }

    return (
        <section className="p-4 bg-white rounded shadow mb-4">
            <h2 className="text-xl font-bold mb-2">Meteorological interesting studies</h2>
            <p>Meteorological interesting studies in the Spanish territory.</p>

            {/*EVOL PREC PRESS*/}
            <AccordionCustomOpen title={interestingStudiesValues.precAndPressEvol.title} id={`interesting_studies-${interestingStudiesValues.precAndPressEvol.name}`}>
                <SelectSearchCustom options={interestingStudiesValues.precAndPressEvol.locationValues} childrenFn={({value}: Option) => (
                    interestingStudiesValues.precAndPressEvol.temporalValues.map((temporal, temporalIdx) => (
                        <AccordionCustomOpenIframe
                            key={temporalIdx}
                            title={temporal.title}
                            src={`https://c0nf1cker.net/interesting_studies/${interestingStudiesValues.precAndPressEvol.name}/${value}/${temporal.name}/plot.html`}
                            id={`interesting_studies-${interestingStudiesValues.precAndPressEvol.name}-${value}-${temporal.name}`}
                        ></AccordionCustomOpenIframe>
                    ))
                )}></SelectSearchCustom>
            </AccordionCustomOpen>

            {/*TOP 10 STATES*/}
            <AccordionCustomOpen title={interestingStudiesValues.top10States.title} id={`interesting_studies-${interestingStudiesValues.top10States.name}`}>
            {
                interestingStudiesValues.top10States.values.map((study, studyIdx) => (
                    <AccordionCustomOpenIframe
                        key={studyIdx}
                        title={study.title}
                        src={`https://c0nf1cker.net/interesting_studies/${interestingStudiesValues.top10States.name}/${study.name}/plot.html`}
                        id={`interesting_studies-${interestingStudiesValues.top10States.name}-${study.name}`}
                    ></AccordionCustomOpenIframe>
                ))
            }
            </AccordionCustomOpen>

        </section>
    )
}
'use client';

import AccordionCustomOpen from "@/components/custom-ui/accordion-custom-open";
import AccordionCustomOpenIframe from "@/components/custom-ui/accordion-custom-open-iframe";

export default function ClimographStudies() {

    const locationValues = {
        peninsula: {
            locationName: "peninsula",
            locationTitle: "Peninsula location",
            locationSubtitle: "Climograph of the peninsular region"
        },
        balearIslands: {
            locationName: "balear_islands",
            locationTitle: "Balear islands",
            locationSubtitle: "Climograph of the balear islands region"
        },
        canaryIslands: {
            locationName: "canary_islands",
            locationTitle: "Canary islands location",
            locationSubtitle: "Climograph of the canary islands region"
        }
    }

    const climateValues = {
        climates: [
            {
                climateName: "arid",
                climateTitle: "Arid climates",
                subclimates: [
                    {
                        climateName: "BSh",
                        subclimateTitle: "BSh",
                        subclimateSubtitle: "BSh: hot steppe climate",
                        locations: [
                            locationValues.peninsula,
                            locationValues.balearIslands,
                            locationValues.canaryIslands
                        ]
                    },
                    {
                        climateName: "BSk",
                        subclimateTitle: "BSk",
                        subclimateSubtitle: "BSk: cold steppe climate",
                        locations: [
                            locationValues.peninsula,
                            locationValues.balearIslands,
                            locationValues.canaryIslands
                        ]
                    },
                    {
                        climateName: "BWh",
                        subclimateTitle: "BWh",
                        subclimateSubtitle: "BWh: warm desert climate",
                        locations: [
                            locationValues.peninsula,
                            locationValues.canaryIslands
                        ]
                    },
                    {
                        climateName: "BWk",
                        subclimateTitle: "BWk",
                        subclimateSubtitle: "BWk: cold desert climate",
                        locations: [
                            locationValues.peninsula
                        ]
                    },
                ]
            },
            {
                climateName: "warm",
                climateTitle: "Warm climates",
                subclimates: [
                    {
                        climateName: "Cfa",
                        subclimateTitle: "Cfa",
                        subclimateSubtitle: "Cfa: warm climate without dry season and with a hot summer",
                        locations: [
                            locationValues.peninsula
                        ]
                    },
                    {
                        climateName: "Cfb",
                        subclimateTitle: "Cfb",
                        subclimateSubtitle: "Cfb: warm climate without dry season and with mild summer",
                        locations: [
                            locationValues.peninsula
                        ]
                    },
                    {
                        climateName: "Csa",
                        subclimateTitle: "Csa",
                        subclimateSubtitle: "Csa: warm climate with dry and hot summer",
                        locations: [
                            locationValues.peninsula,
                            locationValues.balearIslands,
                            locationValues.canaryIslands
                        ]
                    },
                    {
                        climateName: "Csb",
                        subclimateTitle: "Csb",
                        subclimateSubtitle: "Csb: warm climate with dry and mild summer",
                        locations: [
                            locationValues.peninsula,
                            locationValues.balearIslands,
                            locationValues.canaryIslands
                        ]
                    },
                ]
            },
            {
                climateName: "cold",
                climateTitle: "Cold climates",
                subclimates: [
                    {
                        climateName: "Dfb",
                        subclimateTitle: "Dfb",
                        subclimateSubtitle: "Dfb: cold and without dry season climate",
                        locations: [
                            locationValues.peninsula
                        ]
                    },
                    {
                        climateName: "Dfc",
                        subclimateTitle: "Dfc",
                        subclimateSubtitle: "Dfc: cold, without dry season and with a cool summer climate",
                        locations: [
                            locationValues.peninsula
                        ]
                    },
                    {
                        climateName: "Dsb",
                        subclimateTitle: "Dsb",
                        subclimateSubtitle: "Dsb: cold and with a dry summer climate",
                        locations: [
                            locationValues.peninsula
                        ]
                    }
                ]
            },
        ]
    }

    return (
        <section className="p-4 bg-white rounded shadow mb-4">
            <h2 className="text-xl font-bold mb-2">Climograph Studies</h2>
            <p>Studies on the climographs of the different climates of the Spanish territory as of 2024.</p>
            {
                climateValues.climates.map((climate, climateIdx) => (
                    <AccordionCustomOpen key={climateIdx} title={climate.climateTitle} id={`climographs-${climate.climateName}`}>
                    {
                        climate.subclimates.map((subclimate, subclimateIdx) => (
                            <AccordionCustomOpen key={subclimateIdx} title={subclimate.subclimateTitle} subtitle={subclimate.subclimateSubtitle} id={`climographs-${climate.climateName}-${subclimate.climateName}`}>
                            {
                                subclimate.locations.map((location, locationIdx) => (
                                    <AccordionCustomOpenIframe key={locationIdx} title={location.locationTitle} subtitle={location.locationSubtitle} src={`https://c0nf1cker.net/climograph/${climate.climateName}/${subclimate.climateName}/${location.locationName}/plot.html`} id={`climographs-${climate.climateName}-${subclimate.climateName}-${location.locationName}`}>
                                    </AccordionCustomOpenIframe>
                                ))
                            }
                            </AccordionCustomOpen>
                        ))
                    }
                    </AccordionCustomOpen>
                ))
            }
        </section>
    )
}
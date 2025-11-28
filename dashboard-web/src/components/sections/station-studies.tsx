import AccordionCustomOpenIframe from "@/components/custom-ui/accordion-custom-open-iframe";

export default function StationStudies() {

    return (
        <section className="p-4 bg-white rounded shadow mb-4">
            <h2 className="text-xl font-bold mb-2">Meteorological Station Studies</h2>
            <p>Studies on the distribution of meteorological stations across Spanish territory between 1973 and
                2024.</p>

            <AccordionCustomOpenIframe
                title={"Stations count by state"}
                subtitle={"Number of stations per state as of 2024."}
                src={"https://c0nf1cker.net/stations/count_by_state_2024/plot.html"}
                id={"stations-count_by_state_2024"}
            >
            </AccordionCustomOpenIframe>

            <AccordionCustomOpenIframe
                title={"Station count by altitude"}
                subtitle={"Number of stations distributed by altitude as of 2024."}
                src={"https://c0nf1cker.net/stations/count_by_altitude_2024/plot.html"}
                id={"stations-count_by_altitude_2024"}
            >
            </AccordionCustomOpenIframe>

            <AccordionCustomOpenIframe
                title={"Station count evolution"}
                subtitle={"Evolution of the number of stations from 1973 to 2024."}
                src={"https://c0nf1cker.net/stations/count_evol/plot.html"}
                id={"stations-count_evol"}
            >
            </AccordionCustomOpenIframe>
        </section>
    )
}
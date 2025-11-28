import AccordionCustomOpen from "@/components/custom-ui/accordion-custom-open";
import AccordionCustomOpenIframe from "@/components/custom-ui/accordion-custom-open-iframe";

export default function ClimographStudies() {

    return (
        <section className="p-4 bg-white rounded shadow mb-4">
            <h2 className="text-xl font-bold mb-2">Climograph Studies</h2>
            <p>Studies on the climographs of the different climates of the Spanish territory as of 2024.</p>

            <AccordionCustomOpen
                title={"Arid climates"}
                id={"climographs-arid"}
            >
                <AccordionCustomOpen
                    title={"BSh"}
                    subtitle={"BSh: semi-arid climate"}
                    id={"climographs-arid-BSh"}
                >
                    <AccordionCustomOpenIframe
                        title={"Peninsula location"}
                        subtitle={"Climograph of the peninsular region"}
                        src={"https://c0nf1cker.net/climograph/arid/BSh/peninsula/plot.html"}
                        id={"climographs-arid-BSh-peninsula"}
                    ></AccordionCustomOpenIframe>

                    <AccordionCustomOpenIframe
                        title={"Balear islands location"}
                        subtitle={"Climograph of the balear islands region"}
                        src={"https://c0nf1cker.net/climograph/arid/BSh/balear_islands/plot.html"}
                        id={"climographs-arid-BSh-balear_islands"}
                    ></AccordionCustomOpenIframe>

                    <AccordionCustomOpenIframe
                        title={"Canary islands location"}
                        subtitle={"Climograph of the canary islands region"}
                        src={"https://c0nf1cker.net/climograph/arid/BSh/canary_islands/plot.html"}
                        id={"climographs-arid-BSh-canary_islands"}
                    ></AccordionCustomOpenIframe>
                </AccordionCustomOpen>

                <AccordionCustomOpen
                    title={"BSk"}
                    subtitle={"BSk: cold steppe climate"}
                    id={"climographs-arid-BSh"}
                >
                </AccordionCustomOpen>

                <AccordionCustomOpen
                    title={"BWh"}
                    subtitle={"BWh: warm desert climate"}
                    id={"climographs-arid-BSh"}
                >
                </AccordionCustomOpen>

                <AccordionCustomOpen
                    title={"BWk"}
                    subtitle={"BWk: cold desert climate"}
                    id={"climographs-arid-BSh"}
                >
                </AccordionCustomOpen>
            </AccordionCustomOpen>
        </section>
    )
}
use fail::FailScenario;

pub struct CustomFailScenario<'a> {
    _scenario: FailScenario<'a>,
    failpoints: Vec<(String, String)>,
}

impl<'a> CustomFailScenario<'a> {
    pub fn setup(failpoints: &[(&str, &str)]) -> CustomFailScenario<'a> {
        let scenario = FailScenario::setup();
        let failpoints = failpoints
            .iter()
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .collect::<Vec<_>>();

        for (failpoint, action) in failpoints.iter() {
            fail::cfg(failpoint, action).unwrap()
        }

        Self {
            _scenario: scenario,
            failpoints,
        }
    }

    pub fn teardown(self) {
        drop(self);
    }
}

impl<'a> Drop for CustomFailScenario<'a> {
    fn drop(&mut self) {
        for (failpoint, _) in self.failpoints.iter() {
            fail::cfg(failpoint, "off").unwrap()
        }
    }
}

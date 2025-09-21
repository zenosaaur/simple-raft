#[derive(Debug, Clone, Copy)]
enum State {
    NotValid,
    PartialValid,
    Valid,
}

struct Fsm {
    state: State,
}

impl Fsm {
    fn new() -> Self {
        Self {
            state: State::NotValid,
        }
    }

    fn goto(&mut self, s: State) {
        self.state = s;
    }

    fn step(&mut self, command: &str) {
        match self.state {
            State::NotValid => {
                if matches!(command, "exit") {
                    self.goto(State::Valid);
                } else if matches!(command, "GET" | "SET" | "POP") {
                    self.goto(State::PartialValid);
                }
            }
            State::PartialValid => {
                if command.contains(':') {
                    self.goto(State::Valid);
                } else {
                    self.goto(State::NotValid);
                }
            }
            State::Valid => { /* stay valid or add more rules */ }
        }
    }
}

pub fn validate(command: &str) -> bool {
    let mut fsm = Fsm::new();
    if command != "exit" {
        for part in command.split(' ') {
            fsm.step(part);
        }
    } else {
        fsm.step("exit");
    }

    //println!("{:?}",fsm.state);
    matches!(fsm.state, State::Valid)
}
